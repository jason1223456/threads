#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 27 04:05:07 2026

@author: chenguanting
"""

import time
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
# API TOKEN (寫死版)
# =======================================================
API_TOKEN = "bscU4YK22+OYofSoh105OuVJZAh4tsYWZhKawi7WKjY="
API_DOMAIN = "https://api.threadslytics.com/v1"
HEADERS = {"Authorization": f"Bearer {API_TOKEN}"}
REQ_TIMEOUT = 60

# 時區設定
TAIPEI_OFFSET = timedelta(hours=8)

# =======================================================
# PostgreSQL（寫死版，但改成 lazy connect）
# =======================================================
DATABASE_URL = (
    "postgresql://root:" "L2em9nY8K4PcxCuXV60tf1Hs5MG7j3Oz" "@sfo1.clusters.zeabur.com:30599/zeabur"
)

_conn = None
_cursor = None

def get_db():
    """需要用 DB 時才連線，避免 gunicorn import 階段直接爆掉"""
    global _conn, _cursor
    if _conn is not None and _cursor is not None:
        return _conn, _cursor

    _conn = psycopg.connect(DATABASE_URL, row_factory=dict_row)
    _cursor = _conn.cursor()
    return _conn, _cursor

# =======================================================
# Gmail 設定（寫死版）
# =======================================================
SMTP_USER = "jason91082500@gmail.com"
SMTP_PASS = "rwundvtaybzrgzlz"
SMTP_TO = "leona@brainmax-marketing.com"

def send_email(subject, body):
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
# DB: 修正 sequence（解 social_posts_pkey 重複）
# =======================================================
def fix_id_sequence(table_name: str, id_col: str = "id"):
    try:
        conn, cursor = get_db()

        cursor.execute("SELECT pg_get_serial_sequence(%s, %s) AS seq", (table_name, id_col))
        row = cursor.fetchone()
        seq_name = row["seq"] if row else None

        if not seq_name:
            print(f"ℹ️ {table_name}.{id_col} 沒有 serial sequence，略過修正")
            return

        cursor.execute(f"SELECT COALESCE(MAX({id_col}), 1) AS max_id FROM {table_name}")
        max_id = cursor.fetchone()["max_id"]

        cursor.execute("SELECT setval(%s, %s, false)", (seq_name, int(max_id) + 1))
        conn.commit()

        print(f"✅ 已修正 sequence：{seq_name} -> next id = {int(max_id) + 1}")

    except Exception as e:
        print("❌ 修正 sequence 失敗：", e)
        try:
            conn, _ = get_db()
            conn.rollback()
        except:
            pass

# =======================================================
# API FUNCTIONS（加一點 retry，避免偶發 timeout）
# =======================================================
session = requests.Session()

def api_get_json(url, params=None, retries=3):
    last = None
    for i in range(1, retries + 1):
        try:
            r = session.get(url, headers=HEADERS, params=params, timeout=REQ_TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            sleep_s = 1.5 ** (i - 1)
            print(f"⚠️ API 失敗重試 {i}/{retries}: {e} (sleep {sleep_s:.1f}s)")
            time.sleep(sleep_s)
    raise last

def get_keyword_groups():
    return api_get_json(f"{API_DOMAIN}/keyword-groups")["data"]

def get_posts_by_group(group_id):
    posts = []
    page = 1
    while True:
        data = api_get_json(
            f"{API_DOMAIN}/keyword-groups/analytics/{group_id}",
            params={"metricDays": 7, "page": page},
        )
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
# DB FUNCTIONS (social_posts 原本貼文表)
# =======================================================
def get_existing_post(permalink):
    try:
        conn, cursor = get_db()
        cursor.execute("SELECT 1 FROM social_posts WHERE permalink=%s LIMIT 1", (permalink,))
        return cursor.fetchone()
    except:
        try:
            conn, _ = get_db()
            conn.rollback()
        except:
            pass
        return None

def upsert_post(post, metrics):
    """
    保留原本 social_posts 行為：permalink 唯一，一篇貼文只存一筆
    """
    try:
        conn, cursor = get_db()

        post_utc = datetime.fromisoformat(post["postCreatedAt"].replace("Z", "+00:00"))
        post_tw = (post_utc + TAIPEI_OFFSET).replace(tzinfo=None)
        now_tw = (datetime.utcnow() + TAIPEI_OFFSET).replace(tzinfo=None)

        permalink = post["permalink"]
        exists = get_existing_post(permalink)

        if exists:
            cursor.execute("""
                UPDATE social_posts
                SET keyword=%s, content=%s, poster_name=%s,
                    media_title='threads', media_name='threads',
                    site='THREADS', channel='threads專案', api_source='threadslytics',
                    threads_like_count=%s, threads_comment_count=%s,
                    threads_share_count=%s, threads_repost_count=%s,
                    threads_topic=%s, updated_at=%s
                WHERE permalink=%s
            """, (
                post.get("keywordText"), post.get("caption"), post.get("username"),
                metrics["likeCount"], metrics["directReplyCount"],
                metrics["shares"], metrics["repostCount"],
                post.get("tagHeader"), now_tw, permalink
            ))
            conn.commit()
            return "update"

        cursor.execute("""
            INSERT INTO social_posts (
                date, keyword, content, permalink, poster_name,
                media_title, media_name, site, channel, api_source,
                threads_like_count, threads_comment_count,
                threads_share_count, threads_repost_count,
                threads_topic, created_at, updated_at
            )
            VALUES (
                %s, %s, %s, %s, %s,
                'threads','threads','THREADS','threads專案','threadslytics',
                %s, %s, %s, %s,
                %s, %s, %s
            )
        """, (
            post_tw, post.get("keywordText"), post.get("caption"),
            permalink, post.get("username"),
            metrics["likeCount"], metrics["directReplyCount"],
            metrics["shares"], metrics["repostCount"],
            post.get("tagHeader"),
            now_tw, now_tw
        ))
        conn.commit()
        return "insert"

    except Exception as e:
        msg = str(e)
        if "duplicate key value violates unique constraint" in msg and "social_posts_pkey" in msg:
            print("⚠️ social_posts_pkey 重複，修正 sequence 後重試一次…")
            try:
                conn, _ = get_db()
                conn.rollback()
            except:
                pass
            fix_id_sequence("social_posts", "id")
            # 重試一次
            try:
                return upsert_post(post, metrics)
            except Exception as e2:
                print("DB Error (social_posts) retry failed:", e2)
                return "skip"

        print("DB Error (social_posts):", e)
        try:
            conn, _ = get_db()
            conn.rollback()
        except:
            pass
        return "skip"

# =======================================================
# DB FUNCTIONS (social_posts_events 事件表)
# =======================================================
def upsert_event(post, group_name, metrics):
    try:
        conn, cursor = get_db()

        post_utc = datetime.fromisoformat(post["postCreatedAt"].replace("Z", "+00:00"))
        post_tw = (post_utc + TAIPEI_OFFSET).replace(tzinfo=None)
        now_tw = (datetime.utcnow() + TAIPEI_OFFSET).replace(tzinfo=None)

        cursor.execute("""
            INSERT INTO social_posts_events (
                post_time, permalink, code,
                keyword_group, keyword,
                poster_name, content, threads_topic,
                threads_like_count, threads_comment_count,
                threads_share_count, threads_repost_count,
                site, channel, api_source,
                created_at, updated_at
            )
            VALUES (
                %s, %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s,
                'THREADS', 'threads專案', 'threadslytics',
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
# JOBS
# =======================================================
def manual_import_10():
    print("\n===== 🚀 手動匯入 10 筆 =====")
    fix_id_sequence("social_posts", "id")

    total = 0
    groups = get_keyword_groups()
    stats = {}

    for group in groups:
        gname = group.get("groupName", "未知群組")
        posts = get_posts_by_group(group["id"])

        for p in posts:
            if total >= 10:
                break

            metrics = pick_best_metrics(get_metrics(p["code"]))

            result = upsert_post(p, metrics)
            upsert_event(p, gname, metrics)

            if gname not in stats:
                stats[gname] = {"insert": 0, "update": 0, "total": 0}
            if result in ["insert", "update"]:
                stats[gname][result] += 1
                stats[gname]["total"] += 1

            total += 1

        if total >= 10:
            break

    lines = ["【手動匯入前 10 筆】\n"]
    for g, s in stats.items():
        lines.append(f"🔍 關鍵字群組：{g}")
        lines.append(f"📌 時段內貼文數：{s['total']}")
        lines.append(f"🆕 新增：{s['insert']}")
        lines.append(f"🔄 更新：{s['update']}\n")

    send_email("Threads 手動匯入摘要", "\n".join(lines))

def job_import_last_2_to_3_hours():
    print("\n===== ⏰ 每小時 Threads 匯入 =====")
    fix_id_sequence("social_posts", "id")

    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=3)
    end = now - timedelta(hours=2)

    start_tw = (start + TAIPEI_OFFSET).replace(tzinfo=None)
    end_tw = (end + TAIPEI_OFFSET).replace(tzinfo=None)

    lines = [
        f"時間區間：{start_tw.strftime('%Y-%m-%d %H:%M:%S')} ～ {end_tw.strftime('%Y-%m-%d %H:%M:%S')}\n"
    ]

    groups = get_keyword_groups()

    for group in groups:
        gname = group.get("groupName", "未知群組")
        posts = get_posts_by_group(group["id"])

        stat = {"insert": 0, "update": 0, "total": 0}

        for p in posts:
            t = datetime.fromisoformat(p["postCreatedAt"].replace("Z", "+00:00"))
            if not (start <= t <= end):
                continue

            metrics = pick_best_metrics(get_metrics(p["code"]))
            result = upsert_post(p, metrics)
            upsert_event(p, gname, metrics)

            if result in ["insert", "update"]:
                stat[result] += 1
                stat["total"] += 1

        if stat["total"] == 0:
            continue

        lines.append(f"🔍 關鍵字群組：{gname}")
        lines.append(f"📌 時段內貼文數：{stat['total']}")
        lines.append(f"🆕 新增：{stat['insert']}")
        lines.append(f"🔄 更新：{stat['update']}\n")

    send_email("Threads 每小時匯入摘要", "\n".join(lines))

# =======================================================
# Flask + Scheduler（放在 create_app 裡，避免 import 就啟動）
# =======================================================
def create_app():
    app = Flask(__name__)

    scheduler = BackgroundScheduler()
    scheduler.add_job(job_import_last_2_to_3_hours, "cron", minute=0)
    scheduler.add_job(manual_import_10, "date", run_date=datetime.utcnow() + timedelta(seconds=5))
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
        return "Threads Crawler Running"

    return app

# gunicorn 入口
app = create_app()

if __name__ == "__main__":
    # 本機執行用
    app.run(host="0.0.0.0", port=5000)
