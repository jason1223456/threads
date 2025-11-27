import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta, timezone
import base64

# ============================================
# Base64 Token 設定（安全版）
# ============================================
TOKEN_BASE64 = "QmVhcmVyIGJzY1U0WUsyMitPWU9mU29oMTA1T3VWSkFoNHRzWVdaaEthd2k3V0tqWT0="

def get_token():
    """Base64 解碼，取得真正 Bearer Token"""
    return base64.b64decode(TOKEN_BASE64.encode()).decode()

# API Header 使用自動解碼的 Token
HEADERS = {
    "Authorization": get_token()
}

# ============================================
# Threadslytics API
# ============================================
API_DOMAIN = "https://api.threadslytics.com/v1"
TAIPEI_OFFSET = timedelta(hours=8)

# ============================================
# PostgreSQL 連線
# ============================================
conn = psycopg2.connect(
    "postgresql://root:L2em9nY8K4PcxCuXV60tf1Hs5MG7j3Oz@sfo1.clusters.zeabur.com:30599/zeabur"
)
cursor = conn.cursor(cursor_factory=RealDictCursor)

# ============================================
# 取得 keyword groups
# ============================================
def get_keyword_groups():
    r = requests.get(f"{API_DOMAIN}/keyword-groups", headers=HEADERS)
    r.raise_for_status()
    return r.json()["data"]

# ============================================
# 抓某 group 所有貼文
# ============================================
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

# ============================================
# 取得 metrics
# ============================================
def get_metrics(code):
    r = requests.get(
        f"{API_DOMAIN}/threads/post/metrics",
        headers=HEADERS,
        params={"code": code}
    )
    r.raise_for_status()
    return r.json().get("data", [])

# ============================================
# 選 metrics 中「最有數據」的一筆
# ============================================
def pick_best_metrics(metrics):
    if not metrics:
        return {"likeCount": 0, "directReplyCount": 0, "shares": 0, "repostCount": 0}

    for m in metrics:
        if any([
            m.get("likeCount", 0) > 0,
            m.get("directReplyCount", 0) > 0,
            m.get("shares", 0) > 0,
            m.get("repostCount", 0) > 0
        ]):
            return m

    return metrics[0]

# ============================================
# DB 查貼文是否存在
# ============================================
def get_existing_post(permalink):
    cursor.execute(
        "SELECT * FROM social_posts WHERE permalink = %s LIMIT 1",
        (permalink,)
    )
    return cursor.fetchone()

# ============================================
# Insert / Update
# ============================================
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
        print(f"🔄 更新：{post['code']}（like={metrics['likeCount']}）")

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
        print(f"🆕 新增：{post['code']}（like={metrics['likeCount']}）")

# ============================================
# 每小時：抓 2–3 小時前貼文
# ============================================
def job_hourly():
    print("\n⏰ [每小時] 抓取 2–3 小時前的新貼文")

    now_utc = datetime.now(timezone.utc)
    start_time = now_utc - timedelta(hours=3)
    end_time = now_utc - timedelta(hours=2)

    for group in get_keyword_groups():
        for p in get_posts_by_group(group["id"]):
            t = datetime.fromisoformat(p["postCreatedAt"].replace("Z", "+00:00"))

            if start_time <= t <= end_time:
                metrics = pick_best_metrics(get_metrics(p["code"]))
                upsert_post(p, metrics)

# ============================================
# 每 12 小時：補抓 48 小時
# ============================================
def job_refresh():
    print("\n🔁 [每 12 小時] 補抓 48 小時資料")

    now_utc = datetime.now(timezone.utc)
    start_time = now_utc - timedelta(hours=48)

    for group in get_keyword_groups():
        for p in get_posts_by_group(group["id"]):
            t = datetime.fromisoformat(p["postCreatedAt"].replace("Z", "+00:00"))

            if t >= start_time:
                metrics = pick_best_metrics(get_metrics(p["code"]))
                upsert_post(p, metrics)


# ============================================
# Flask + Scheduler
# ============================================
app = Flask(__name__)
scheduler = BackgroundScheduler()

scheduler.add_job(job_hourly, "cron", minute=0)
scheduler.add_job(job_refresh, "cron", hour="0,12")
scheduler.start()

@app.route("/")
def index():
    return "Threads Crawler Running (Base64 Token Enabled)"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
