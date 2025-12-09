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
DATABASE_URL = (
    "postgresql://root:"
    "L2em9nY8K4PcxCuXV60tf1Hs5MG7j3Oz"
    "@sfo1.clusters.zeabur.com:30599/zeabur"
)

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
# DB FUNCTIONS — channel 永遠寫 threads專案
# =======================================================
def get_existing_post(permalink):
    try:
        cursor.execute(
            "SELECT 1 FROM social_posts WHERE permalink=%s LIMIT 1",
            (permalink,)
        )
        return cursor.fetchone()
    except Exception:
        conn.rollback()
        return None

def upsert_post(post, metrics):
    try:
        post_time_utc = datetime.fromisoformat(
            post["postCreatedAt"].replace("Z", "+00:00")
        )
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
                    channel='threads專案',
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
            print(f"🔄 更新：{post['code']}")

        else:
            cursor.execute("""
                INSERT INTO social_posts (
                    date, keyword, content, permalink, poster_name,
                    media_title, media_name, site, channel,
                    threads_like_count, threads_comment_count,
                    threads_share_count, threads_repost_count,
                    threads_topic, created_at, updated_at
                )
                VALUES (%s,%s,%s,%s,%s,
                        'threads','threads','THREADS','threads專案',
                        %s,%s,%s,%s,%s,%s,%s)
            """, (
                post_time_taipei,
                post.get("keywordText"),
                post.get("caption"),
                post.get("permalink"),
                post.get("username"),               # ← 正確 poster_name
                metrics["likeCount"],
                metrics["directReplyCount"],
                metrics["shares"],
                metrics["repostCount"],
                post.get("tagHeader"),
                now_taipei,
                now_taipei
            ))
            print(f"🆕 新增：{post['code']}")

        conn.commit()

    except Exception as e:
        print("❌ 寫入錯誤 — rollback")
        print(e)
        conn.rollback()

# =======================================================
# 手動匯入 — 前 10 筆
# =======================================================
def manual_import_10():
    print("\n===== 🚀 手動匯入 10 筆貼文 → social_posts =====")

    total = 0

    for group in get_keyword_groups():
        posts = get_posts_by_group(group["id"])
        for p in posts:
            if total >= 10:
                print("\n🎉 已完成匯入 10 筆")
                return
            metrics = pick_best_metrics(get_metrics(p["code"]))
            upsert_post(p, metrics)
            total += 1
            print(f"🆕 第 {total} 筆：{p['code']}")

# =======================================================
# ⭐ 定時排程 — 每小時整點 → 抓前 3~2 小時貼文
# =======================================================
def job_import_last_2_to_3_hours():
    print("\n⏰ 定時任務：抓前 3～2 小時貼文 → social_posts")

    now = datetime.now(timezone.utc)
    start_time = now - timedelta(hours=3)
    end_time = now - timedelta(hours=2)

    total = 0

    for group in get_keyword_groups():
        posts = get_posts_by_group(group["id"])

        for p in posts:
            t = datetime.fromisoformat(p["postCreatedAt"].replace("Z", "+00:00"))

            if start_time <= t <= end_time:
                metrics = pick_best_metrics(get_metrics(p["code"]))
                upsert_post(p, metrics)
                total += 1

    print(f"✨ 本次排程匯入 {total} 筆（{start_time} ～ {end_time}）")

# =======================================================
# Flask + APScheduler
# =======================================================
app = Flask(__name__)
scheduler = BackgroundScheduler()

scheduler.add_job(job_import_last_2_to_3_hours, "cron", minute=0)

scheduler.add_job(
    manual_import_10,
    "date",
    run_date=datetime.utcnow() + timedelta(seconds=5)
)

scheduler.start()

@app.route("/health")
def health():
    return "OK", 200

@app.route("/")
def index():
    return "Threads SocialPosts Crawler Running"

if __name__ == "__main__":
    manual_import_10()
    app.run(host="0.0.0.0", port=5000)
