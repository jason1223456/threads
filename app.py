import requests
import base64
import psycopg
from psycopg.rows import dict_row
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta, timezone

# =======================================================
# API TOKEN（內嵌 Base64，不使用 .env）
# =======================================================
# ⚠️ 你把真正 Token 做 Base64 後放到這裡即可
THREADS_TOKEN_B64 = "YnNjVTRZS0IyMytPWW9mU29oMTA1T3VWSlpBaDR0c1lXWmhLYXdpN1dLejE9"

# 解碼取得真正 Token
API_TOKEN = base64.b64decode(THREADS_TOKEN_B64).decode().strip()

API_DOMAIN = "https://api.threadslytics.com/v1"
HEADERS = {"Authorization": f"Bearer {API_TOKEN}"}

TAIPEI_OFFSET = timedelta(hours=8)

# =======================================================
# PostgreSQL (psycopg3)
# =======================================================
DATABASE_URL = "postgresql://root:L2em9nY8K4PcxCuXV60tf1Hs5MG7j3Oz@sfo1.clusters.zeabur.com:30599/zeabur"

conn = psycopg.connect(DATABASE_URL, row_factory=dict_row)
cursor = conn.cursor()

# =======================================================
# 取得 keyword groups
# =======================================================
def get_keyword_groups():
    r = requests.get(f"{API_DOMAIN}/keyword-groups", headers=HEADERS)
    r.raise_for_status()
    return r.json()["data"]

# =======================================================
# 抓 group 底下所有貼文
# =======================================================
def get_posts_by_group(group_id):
    posts = []
    page = 1

    while True:
        r = requests.get(
            f"{API_DOMAIN}/keyword-groups/analytics/{group_id}",
            headers=HEADERS,
            params={"metricDays": 7, "page": page}
        )
        r.raise_for_status()
        chunk = r.json().get("posts", [])

        if not chunk:
            break

        posts.extend(chunk)
        page += 1

    return posts

# =======================================================
# 抓 metrics
# =======================================================
def get_metrics(code):
    r = requests.get(
        f"{API_DOMAIN}/threads/post/metrics",
        headers=HEADERS,
        params={"code": code}
    )
    r.raise_for_status()
    return r.json().get("data", [])

# =======================================================
# 修正版：把 null 變 0
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
        return {
            "likeCount": 0,
            "directReplyCount": 0,
            "shares": 0,
            "repostCount": 0
        }

    for m in metrics:
        nm = normalize_metrics(m)
        if any([nm["likeCount"], nm["directReplyCount"], nm["shares"], nm["repostCount"]]):
            return nm

    return normalize_metrics(metrics[0])

# =======================================================
# 查 DB 是否已存在
# =======================================================
def get_existing_post(permalink):
    cursor.execute(
        "SELECT * FROM social_posts WHERE permalink=%s LIMIT 1",
        (permalink,)
    )
    return cursor.fetchone()

# =======================================================
# Insert / Update
# =======================================================
def upsert_post(post, metrics):
    post_time_utc = datetime.fromisoformat(post["postCreatedAt"].replace("Z", "+00:00"))
    post_time_taipei = (post_time_utc + TAIPEI_OFFSET).replace(tzinfo=None)

    now_taipei = (datetime.now(timezone.utc) + TAIPEI_OFFSET).replace(tzinfo=None)

    existing = get_existing_post(post["permalink"])

    if existing:
        cursor.execute("""
            UPDATE social_posts
            SET threads_like_count=%s,
                threads_comment_count=%s,
                threads_share_count=%s,
                threads_repost_count=%s,
                updated_at=%s
            WHERE permalink=%s
        """, (
            metrics["likeCount"],
            metrics["directReplyCount"],
            metrics["shares"],
            metrics["repostCount"],
            now_taipei,
            post["permalink"]
        ))

        conn.commit()
        print(f"🔄 更新：{post['code']} (like={metrics['likeCount']})")

    else:
        cursor.execute("""
            INSERT INTO social_posts (
                date, keyword, content, permalink, poster_name,
                media_title, media_name, site, channel,
                threads_like_count, threads_comment_count,
                threads_share_count, threads_repost_count,
                threads_topic, created_at, updated_at
            )
            VALUES (%s,%s,%s,%s,%s,'threads','threads','THREADS','threads專案',
                %s,%s,%s,%s,%s,%s,%s)
        """, (
            post_time_taipei,
            post.get("keywordText"),
            post.get("caption"),
            post.get("permalink"),
            post.get("username"),

            metrics["likeCount"],
            metrics["directReplyCount"],
            metrics["shares"],
            metrics["repostCount"],
            post.get("tagHeader"),
            now_taipei,
            now_taipei
        ))

        conn.commit()
        print(f"🆕 新增：{post['code']} (like={metrics['likeCount']})")

# =======================================================
# 每小時抓 2–3 小時前貼文
# =======================================================
def job_hourly():
    print("\n⏰ 每小時任務執行")

    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=3)
    end = now - timedelta(hours=2)

    for group in get_keyword_groups():
        for p in get_posts_by_group(group["id"]):
            t = datetime.fromisoformat(p["postCreatedAt"].replace("Z", "+00:00"))
            if start <= t <= end:
                metrics = pick_best_metrics(get_metrics(p["code"]))
                upsert_post(p, metrics)

# =======================================================
# 每 12 小時補抓 48 小時內貼文
# =======================================================
def job_refresh():
    print("\n🔁 48 小時補抓任務執行")

    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=48)

    for group in get_keyword_groups():
        for p in get_posts_by_group(group["id"]):
            t = datetime.fromisoformat(p["postCreatedAt"].replace("Z", "+00:00"))
            if t >= start:
                metrics = pick_best_metrics(get_metrics(p["code"]))
                upsert_post(p, metrics)

# =======================================================
# Flask + APScheduler
# =======================================================
app = Flask(__name__)
scheduler = BackgroundScheduler()

scheduler.add_job(job_hourly, "cron", minute=0)
scheduler.add_job(job_refresh, "cron", hour="0,12")
scheduler.start()

@app.route("/")
def index():
    return "Threads Crawler is running (psycopg3)"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
