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
# ✅ 你只要填這裡（把你原本的值貼進來）
# =======================================================
API_TOKEN = "bscU4YK22+OYofSoh105OuVJZAh4tsYWZhKawi7WKjY="

DATABASE_URL = ( "postgresql://root:" "L2em9nY8K4PcxCuXV60tf1Hs5MG7j3Oz" "@sfo1.clusters.zeabur.com:30599/zeabur" )

SMTP_USER = "jason91082500@gmail.com" 
SMTP_PASS = "rwundvtaybzrgzlz" 
SMTP_TO = "leona@brainmax-marketing.com"

# =======================================================
# 固定設定
# =======================================================
API_DOMAIN = "https://api.threadslytics.com/v1"
HEADERS = {"Authorization": f"Bearer {API_TOKEN}"}
TAIPEI_OFFSET = timedelta(hours=8)

# requests timeout
# connect timeout 固定 10s；read timeout 依此變數
REQ_TIMEOUT = 60

# =======================================================
# Lazy DB connection（避免 gunicorn import 就連 DB 爆掉）
# =======================================================
_conn = None
_cursor = None

def get_db():
    global _conn, _cursor
    if _conn is not None and _cursor is not None:
        return _conn, _cursor

    _conn = psycopg.connect(DATABASE_URL, row_factory=dict_row)
    _cursor = _conn.cursor()
    return _conn, _cursor

# =======================================================
# Gmail
# =======================================================
def send_email(subject, body):
    # 若你暫時不想寄信：把 SMTP_* 留空即可，自動跳過
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
# HTTP / API（強化：retry + backoff + jitter + 最後不 raise）
# =======================================================
session = requests.Session()

def api_get_json(url, params=None, retries=5):
    """
    - timeout 分成 connect/read
    - 重試 5 次 + 指數退避 + 抖動
    - 最終失敗回傳 None（不要 raise，避免 APScheduler job 整個炸掉）
    """
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
            # 1,2,4,8,16 秒 + jitter（最多 60 秒）
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
# METRICS
# =======================================================
def normalize_metrics(m):
    return {
        "likeCount": m.get("likeCount") or 0,
        "directReplyCount": m.get("directReplyCount") or 0,
        "shares": m.get("shares") or 0,
        "repostCount": m.get("repostCount") or 0
    }

def pick_best_metrics(metrics):
    if not metrics:
        return {"likeCount": 0, "directReplyCount": 0, "shares": 0, "repostCount": 0}
    for m in metrics:
        nm = normalize_metrics(m)
        if any(nm.values()):
            return nm
    return normalize_metrics(metrics[0])

# =======================================================
# DB: events only（重要：你已把 post_time 改成 date，所以用 date）
#      且你的表沒有 channel，所以不寫 channel
# =======================================================
def upsert_event(post, group_name, metrics):
    try:
        conn, cursor = get_db()

        post_utc = datetime.fromisoformat(post["postCreatedAt"].replace("Z", "+00:00"))
        post_tw = (post_utc + TAIPEI_OFFSET).replace(tzinfo=None)
        now_tw = (datetime.utcnow() + TAIPEI_OFFSET).replace(tzinfo=None)

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
            ON CONFLICT (permalink, keyword_group, keyword)
            DO UPDATE SET
                poster_name = EXCLUDED.poster_name,
                content = EXCLUDED.content,
                threads_topic = EXCLUDED.threads_topic,
                threads_like_count = EXCLUDED.threads_like_count,
                threads_comment_count = EXCLUDED.threads_comment_count,
                threads_share_count = EXCLUDED.threads_share_count,
                threads_repost_count = EXCLUDED.threads_repost_count,
                updated_at = EXCLUDED.updated_at
        """, (
            post_tw, post.get("permalink"), post.get("code"),
            group_name, post.get("keywordText"),
            post.get("username"), post.get("caption"), post.get("tagHeader"),
            metrics["likeCount"], metrics["directReplyCount"],
            metrics["shares"], metrics["repostCount"],
            now_tw, now_tw
        ))

        conn.commit()
        return "event_upsert"

    except Exception as e:
        print("DB Error (social_posts_events):", e)
        try:
            conn, _ = get_db()
            conn.rollback()
        except:
            pass
        return "skip"

# =======================================================
# JOB: 手動匯入（前 10 筆）
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

            metrics = pick_best_metrics(get_metrics(p.get("code")))
            result = upsert_event(p, gname, metrics)

            if gname not in stats:
                stats[gname] = {"upsert": 0, "total": 0}

            if result == "event_upsert":
                stats[gname]["upsert"] += 1
                stats[gname]["total"] += 1

            total += 1

        if total >= 10:
            break

    lines = ["【Threads 手動匯入前 10 筆（events only）】\n"]
    for g, s in stats.items():
        lines.append(f"🔍 關鍵字群組：{g}")
        lines.append(f"📌 寫入事件數：{s['total']}")
        lines.append(f"🆙 Upsert：{s['upsert']}\n")

    send_email("Threads 手動匯入摘要（events only）", "\n".join(lines))

# =======================================================
# JOB: 每小時匯入（前 3～2 小時）
# =======================================================
def job_import_last_2_to_3_hours_events_only():
    print("\n===== ⏰ 每小時 Threads 匯入（events only） =====")

    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=3)
    end = now - timedelta(hours=2)

    start_tw = (start + TAIPEI_OFFSET).replace(tzinfo=None)
    end_tw = (end + TAIPEI_OFFSET).replace(tzinfo=None)

    lines = [
        f"時間區間：{start_tw.strftime('%Y-%m-%d %H:%M:%S')} ～ {end_tw.strftime('%Y-%m-%d %H:%M:%S')}\n"
    ]

    groups = get_keyword_groups()
    if not groups:
        print("⚠️ get_keyword_groups() empty (API unstable). Skip this run.")
        return

    for group in groups:
        gname = group.get("groupName", "未知群組")
        posts = get_posts_by_group(group["id"])
        if not posts:
            continue

        stat = {"upsert": 0, "total": 0}

        for p in posts:
            # postCreatedAt 是 UTC
            try:
                t = datetime.fromisoformat(p["postCreatedAt"].replace("Z", "+00:00"))
            except Exception:
                continue

            if not (start <= t < end):
                continue

            metrics = pick_best_metrics(get_metrics(p.get("code")))
            result = upsert_event(p, gname, metrics)

            if result == "event_upsert":
                stat["upsert"] += 1
                stat["total"] += 1

        if stat["total"] == 0:
            continue

        # ✅ 你之前想要「跑完一個群組通知一下」：這行就有
        print(f"✅ Group done: {gname} | events={stat['total']}")

        lines.append(f"🔍 關鍵字群組：{gname}")
        lines.append(f"📌 時段內事件數：{stat['total']}")
        lines.append(f"🆙 Upsert：{stat['upsert']}\n")

    send_email("Threads 每小時匯入摘要（events only）", "\n".join(lines))

# =======================================================
# Flask + Scheduler（放在 create_app 裡，避免 import 就啟動）
# =======================================================
def create_app():
    app = Flask(__name__)

    scheduler = BackgroundScheduler()
    scheduler.add_job(job_import_last_2_to_3_hours_events_only, "cron", minute=0)
    scheduler.add_job(manual_import_10_events_only, "date", run_date=datetime.utcnow() + timedelta(seconds=5))
    scheduler.start()

    @app.route("/health")
    def health():
        # DB 壞掉也不要讓服務起不來
        try:
            conn, cursor = get_db()
            cursor.execute("SELECT 1;")
            return "OK", 200
        except Exception as e:
            return f"DB_NOT_READY: {e}", 200

    @app.route("/")
    def index():
        return "Threads Events Importer Running"

    return app

# gunicorn 入口
app = create_app()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
