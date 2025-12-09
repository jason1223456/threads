import requests
import psycopg
from psycopg.rows import dict_row
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta, timezone

# =======================================================
# API TOKEN
# =======================================================
API_TOKEN = "bscU4YK22+OYofSoh105OuVJZAh4tsYWZhKawi7WKjY="
API_DOMAIN = "https://api.threadslytics.com/v1"
HEADERS = {"Authorization": f"Bearer {API_TOKEN}"}

TAIPEI_OFFSET = timedelta(hours=8)

# =======================================================
# PostgreSQL
# =======================================================
DATABASE_URL = "postgresql://root:L2em9nY8K4PcxCuXV60tf1Hs5MG7j3Oz@sfo1.clusters.zeabur.com:30599/zeabur"
conn = psycopg.connect(DATABASE_URL, row_factory=dict_row)
cursor = conn.cursor()

# =======================================================
# API FUNCTIONS
# =======================================================
def get_keyword_groups():
    r = requests.get(f"{API_DOMAIN}/keyword-groups", headers=HEADERS)
    r.raise_for_status()
    return r.json()["data"]

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

def get_metrics(code):
    r = requests.get(
        f"{API_DOMAIN}/threads/post/metrics",
        headers=HEADERS,
        params={"code": code}
    )
    r.raise_for_status()
    return r.json().get("data", [])

# =======================================================
# METRICS NORMALIZATION
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
        if any([nm["likeCount"], nm["directReplyCount"], nm["shares"], nm["repostCount"]]):
            return nm

    return normalize_metrics(metrics[0])

# =======================================================
# DB FUNCTIONS (WRITE TO social_posts_backup)
# =======================================================
def get_existing_post(permalink):
    cursor.execute(
        "SELECT * FROM social_posts_backup WHERE permalink=%s LIMIT 1",
        (permalink,)
    )
    return cursor.fetchone()

def upsert_post(post, metrics):
    post_time_utc = datetime.fromisoformat(post["postCreatedAt"].replace("Z", "+00:00"))
    post_time_taipei = (post_time_utc + TAIPEI_OFFSET).replace(tzinfo=None)
    now_taipei = (datetime.now(timezone.utc) + TAIPEI_OFFSET).replace(tzinfo=None)

    existing = get_existing_post(post["permalink"])

    if existing:
        cursor.execute("""
            UPDATE social_posts_backup
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
        print(f"🔄 更新（BACKUP）：{post['code']}")

    else:
        cursor.execute("""
            INSERT INTO social_posts_backup (
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
        print(f"🆕 新增（BACKUP）：{post['code']}")

# =======================================================
# 手動補抓：抓所有貼文 → backup
# =======================================================
def manual_import_all():
    print("\n===== 手動補抓所有貼文 → social_posts_backup =====")

    total = 0
    for group in get_keyword_groups():
        print(f"\n🔍 群組：{group['groupName']}")
        posts = get_posts_by_group(group["id"])

        for p in posts:
            metrics = pick_best_metrics(get_metrics(p["code"]))
            upsert_post(p, metrics)
            total += 1

    print(f"\n🎉 完成！共寫入/更新 {total} 筆到 social_posts_backup")

# =======================================================
# 定時任務：每小時寫入最近兩小時貼文
# =======================================================
def job_last_2_hours():
    print("\n⏰ 每小時更新最近兩小時貼文 → social_posts_backup")

    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=2)

    for group in get_keyword_groups():
        for p in get_posts_by_group(group["id"]):
            t = datetime.fromisoformat(p["postCreatedAt"].replace("Z", "+00:00"))
            if t >= start:
                metrics = pick_best_metrics(get_metrics(p["code"]))
                upsert_post(p, metrics)

# =======================================================
# Flask + Scheduler
# =======================================================
app = Flask(__name__)
scheduler = BackgroundScheduler()

scheduler.add_job(job_last_2_hours, "cron", minute=0)
scheduler.start()

@app.route("/")
def index():
    return "Threads BACKUP crawler is running"

# =======================================================
# MAIN
# =======================================================
if __name__ == "__main__":
    manual_import_all()  # ← 手動匯入全部
    app.run(host="0.0.0.0", port=5000)
