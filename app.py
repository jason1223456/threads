#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import random
import requests
import psycopg
from psycopg.rows import dict_row
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta, timezone
import smtplib
from email.mime.text import MIMEText
from email.header import Header

# =======================================================
# ✅ Hardcoded Config (NO ENV)
# =======================================================
API_TOKEN = "bscU4YK22+OYofSoh105OuVJZAh4tsYWZhKawi7WKjY="

DATABASE_URL = ( "postgresql://root:" "L2em9nY8K4PcxCuXV60tf1Hs5MG7j3Oz" "@sfo1.clusters.zeabur.com:30599/zeabur" )

SMTP_USER = "jason91082500@gmail.com" 
SMTP_PASS = "rwundvtaybzrgzlz" 
SMTP_TO = "leona@brainmax-marketing.com"
# =======================================================
# Fixed settings
# =======================================================
API_DOMAIN = "https://api.threadslytics.com/v1"
HEADERS = {"Authorization": f"Bearer {API_TOKEN}"}
TAIPEI_OFFSET = timedelta(hours=8)

# requests timeout
REQ_TIMEOUT = 60  # seconds (read timeout); connect timeout fixed at 10s

# 抓取貼文範圍：最近 24 小時內發文
POST_LOOKBACK_HOURS = 24

# 若啟動時不想自動跑「手動匯入10筆」，改成 False
RUN_MANUAL_IMPORT_ON_START = True

# =======================================================
# ✅ DB helper (better than global cursor sharing)
#   - 不共用 cursor，避免 Flask/Scheduler 在多執行緒或重連時出問題
# =======================================================
_conn = None

def get_conn():
    global _conn
    if _conn is None or _conn.closed:
        _conn = psycopg.connect(DATABASE_URL, row_factory=dict_row)
    return _conn

# =======================================================
# Email (optional)
# =======================================================
def send_email(subject, body):
    # 若不想寄信：把 SMTP_* 留空即可，自動跳過
    if not (SMTP_USER and SMTP_PASS and SMTP_TO):
        print("ℹ️ SMTP not set, skip email")
        return

    try:
        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = Header(subject, "utf-8")
        msg["From"] = SMTP_USER
        msg["To"] = SMTP_TO

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(SMTP_USER, SMTP_TO, msg.as_string())

        print("📧 Email 已送出")
    except Exception as e:
        print("❌ Email 寄送失敗：", e)

# =======================================================
# HTTP / API (retry + backoff + jitter)
# =======================================================
session = requests.Session()

def api_get_json(url, params=None, retries=5):
    for i in range(1, retries + 1):
        try:
            r = session.get(
                url,
                headers=HEADERS,
                params=params,
                timeout=(10, REQ_TIMEOUT),  # (connect, read)
            )
            r.raise_for_status()
            return r.json()
        except Exception as e:
            base = min(60, 2 ** (i - 1))
            jitter = random.uniform(0, 1.0)
            sleep_s = base + jitter
            print(f"⚠️ API error retry {i}/{retries}: {e} (sleep {sleep_s:.1f}s)")
            time.sleep(sleep_s)

    print(f"❌ API failed after {retries} retries: {url}")
    return None

def get_keyword_groups():
    data = api_get_json(f"{API_DOMAIN}/keyword-groups")
    if not data or "data" not in data:
        return []
    return data["data"]

def get_posts_by_group(group_id):
    posts = []
    page = 1
    while True:
        data = api_get_json(
            f"{API_DOMAIN}/keyword-groups/analytics/{group_id}",
            params={"metricDays": 7, "page": page},
        )
        if not data:
            break

        chunk = data.get("posts", [])
        if not chunk:
            break

        posts.extend(chunk)
        page += 1
    return posts

def get_metrics(code):
    data = api_get_json(
        f"{API_DOMAIN}/threads/post/metrics",
        params={"code": code},
    )
    if not data:
        return []
    return data.get("data", [])

# =======================================================
# Metrics helpers
# =======================================================
def normalize_metrics(m):
    return {
        "createdAt": m.get("createdAt"),
        "likeCount": m.get("likeCount") or 0,
        "directReplyCount": m.get("directReplyCount") or 0,
        "shares": m.get("shares") or 0,
        "repostCount": m.get("repostCount") or 0,
        "quotes": m.get("quotes") or 0,
    }

def pick_latest_metrics(metrics):
    """
    Return (metric_time_utc: datetime|None, metrics_dict)
    Choose the newest metrics by createdAt.
    """
    if not metrics:
        return None, {"likeCount": 0, "directReplyCount": 0, "shares": 0, "repostCount": 0, "quotes": 0}

    parsed = []
    for m in metrics:
        nm = normalize_metrics(m)
        ca = nm.get("createdAt")
        if not ca:
            continue
        try:
            t = datetime.fromisoformat(ca.replace("Z", "+00:00"))
        except Exception:
            continue
        parsed.append((t, nm))

    if not parsed:
        # fallback: take first
        first = normalize_metrics(metrics[0])
        return None, first

    parsed.sort(key=lambda x: x[0], reverse=True)
    return parsed[0][0], parsed[0][1]

# =======================================================
# DB logic
#  - 用 created_at 當 metrics 快照時間（台北時間）
#  - 用 updated_at 當寫入時間（台北時間）
#  - 你已經建了 UNIQUE (code, created_at)，所以用 ON CONFLICT DO NOTHING 防重
# =======================================================
def get_latest_snapshot_time_for_code(code: str):
    conn = get_conn()
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT created_at
            FROM social_posts_events
            WHERE code = %s
            ORDER BY created_at DESC
            LIMIT 1
        """, (code,))
        row = cursor.fetchone()
        return row["created_at"] if row else None

def insert_new_metrics_row(post, group_name, snap_time_utc, metrics):
    """
    Insert a NEW row only when snap_time is newer than latest snapshot in DB.
    Stored:
      - date       : post publish time (Taipei)
      - created_at : metric snapshot time (Taipei)
      - updated_at : ingestion time (Taipei)
    """
    conn = get_conn()

    # post publish time (UTC -> Taipei naive)
    try:
        post_utc = datetime.fromisoformat(post["postCreatedAt"].replace("Z", "+00:00"))
    except Exception:
        return "skip_bad_post_time"

    post_tw = (post_utc + TAIPEI_OFFSET).replace(tzinfo=None)

    # snapshot time for metrics (UTC -> Taipei naive)
    now_utc = datetime.now(timezone.utc)
    snap_utc = snap_time_utc or now_utc
    snap_tw = (snap_utc + TAIPEI_OFFSET).replace(tzinfo=None)

    code = post.get("code")
    if not code:
        return "skip_no_code"

    latest = get_latest_snapshot_time_for_code(code)
    if latest and snap_tw <= latest:
        return "skip_old"

    ingest_tw = (datetime.utcnow() + TAIPEI_OFFSET).replace(tzinfo=None)

    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO social_posts_events (
                    date, permalink, code,
                    keyword_group, keyword,
                    poster_name, content, threads_topic,
                    threads_like_count, threads_comment_count,
                    threads_share_count, threads_repost_count,
                    site, api_source,
                    created_at, updated_at
                )
                VALUES (
                    %s, %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s,
                    'THREADS', 'threadslytics',
                    %s, %s
                )
                ON CONFLICT (code, created_at) DO NOTHING
            """, (
                post_tw, post.get("permalink"), code,
                group_name, post.get("keywordText"),
                post.get("username"), post.get("caption"), post.get("tagHeader"),
                metrics["likeCount"], metrics["directReplyCount"],
                metrics["shares"], metrics["repostCount"],
                snap_tw, ingest_tw
            ))
        conn.commit()
        return "inserted"
    except Exception as e:
        print("DB Error (insert social_posts_events):", e)
        try:
            conn.rollback()
        except Exception:
            pass
        return "error"

# =======================================================
# JOB: manual import first 10
# =======================================================
def manual_import_10_events_only():
    print("\n===== 🚀 手動匯入 10 筆（events only） =====")
    total = 0

    groups = get_keyword_groups()
    if not groups:
        print("⚠️ get_keyword_groups() empty. Skip manual import.")
        return

    stats = {}
    for group in groups:
        gname = group.get("groupName", "未知群組")
        posts = get_posts_by_group(group["id"])
        if not posts:
            continue

        for p in posts:
            if total >= 10:
                break

            snap_time_utc, m = pick_latest_metrics(get_metrics(p.get("code")))
            result = insert_new_metrics_row(p, gname, snap_time_utc, m)

            stats.setdefault(gname, {"inserted": 0, "skipped": 0, "error": 0, "total": 0})
            stats[gname]["total"] += 1
            if result == "inserted":
                stats[gname]["inserted"] += 1
            elif result in ("skip_old", "skip_bad_post_time", "skip_no_code"):
                stats[gname]["skipped"] += 1
            else:
                stats[gname]["error"] += 1

            total += 1

        if total >= 10:
            break

    lines = ["【Threads 手動匯入前 10 筆（events only）】\n"]
    for g, s in stats.items():
        lines.append(f"🔍 關鍵字群組：{g}")
        lines.append(f"📌 本次處理：{s['total']}")
        lines.append(f"✅ 新增：{s['inserted']}")
        lines.append(f"⏭️ 跳過：{s['skipped']}")
        lines.append(f"❌ 錯誤：{s['error']}\n")

    send_email("Threads 手動匯入摘要（events only）", "\n".join(lines))

# =======================================================
# JOB: hourly import (rolling last 24h posts)
# =======================================================
def job_import_last_24_hours_events_only():
    print("\n===== ⏰ 每小時 Threads 匯入（最近 24 小時貼文） =====")

    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=POST_LOOKBACK_HOURS)

    start_tw = (start + TAIPEI_OFFSET).replace(tzinfo=None)
    now_tw = (now + TAIPEI_OFFSET).replace(tzinfo=None)

    lines = [f"發文時間範圍：{start_tw:%Y-%m-%d %H:%M:%S} ～ {now_tw:%Y-%m-%d %H:%M:%S}\n"]

    groups = get_keyword_groups()
    if not groups:
        print("⚠️ get_keyword_groups() empty (API unstable). Skip this run.")
        return

    for group in groups:
        gname = group.get("groupName", "未知群組")
        posts = get_posts_by_group(group["id"])
        if not posts:
            continue

        stat = {"inserted": 0, "skipped": 0, "error": 0, "total": 0}

        for p in posts:
            # postCreatedAt 是 UTC
            try:
                post_time_utc = datetime.fromisoformat(p["postCreatedAt"].replace("Z", "+00:00"))
            except Exception:
                continue

            # ✅ 只抓最近 24 小時內發的貼文
            if post_time_utc < start:
                continue

            snap_time_utc, m = pick_latest_metrics(get_metrics(p.get("code")))
            result = insert_new_metrics_row(p, gname, snap_time_utc, m)

            stat["total"] += 1
            if result == "inserted":
                stat["inserted"] += 1
            elif result in ("skip_old", "skip_bad_post_time", "skip_no_code"):
                stat["skipped"] += 1
            else:
                stat["error"] += 1

        if stat["total"] == 0:
            continue

        print(f"✅ Group done: {gname} | total={stat['total']} inserted={stat['inserted']} skipped={stat['skipped']} error={stat['error']}")
        lines.append(f"🔍 關鍵字群組：{gname}")
        lines.append(f"📌 本次處理：{stat['total']}")
        lines.append(f"✅ 新增：{stat['inserted']}")
        lines.append(f"⏭️ 跳過：{stat['skipped']}")
        lines.append(f"❌ 錯誤：{stat['error']}\n")

    send_email("Threads 每小時匯入摘要（最近24h貼文）", "\n".join(lines))

# =======================================================
# Flask + Scheduler
# =======================================================
def create_app():
    app = Flask(__name__)

    scheduler = BackgroundScheduler()

    # ✅ 每小時整點跑一次：rolling 24h 回寫
    scheduler.add_job(job_import_last_24_hours_events_only, "cron", minute=0)

    # 可選：啟動後跑一次手動匯入 10 筆
    if RUN_MANUAL_IMPORT_ON_START:
        scheduler.add_job(manual_import_10_events_only, "date", run_date=datetime.utcnow() + timedelta(seconds=5))

    scheduler.start()

    @app.route("/health")
    def health():
        try:
            conn = get_conn()
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1;")
            return "OK", 200
        except Exception as e:
            return f"DB_NOT_READY: {e}", 200

    @app.route("/")
    def index():
        return "Threads Events Importer Running"

    return app

app = create_app()

if __name__ == "__main__":
    # For local run
    app.run(host="0.0.0.0", port=5000)
