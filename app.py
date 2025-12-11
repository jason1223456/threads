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
# Gmail 設定
# =======================================================
SMTP_USER = "jason91082500@gmail.com"
SMTP_PASS = "rwun dvta ybzr gzlz"  # ⚠ 16碼 Gmail App Password
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
# NORMALIZE METRICS
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
# DB FUNCTIONS
# =======================================================
def get_existing_post(permalink):
    try:
        cursor.execute("SELECT 1 FROM social_posts WHERE permalink=%s LIMIT 1", (permalink,))
        return cursor.fetchone()
    except:
        conn.rollback()
        return None

def upsert_post(post, metrics):
    try:
        post_time_utc = datetime.fromisoformat(post["postCreatedAt"].replace("Z", "+00:00"))
        post_time_taipei = (post_time_utc + TAIPEI_OFFSET).replace(tzinfo=None)
        now_taipei = (datetime.now(timezone.utc) + TAIPEI_OFFSET).replace(tzinfo=None)

        permalink = post["permalink"]
        exists = get_existing_post(permalink)

        # UPDATE
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
                post.get("keywordText"),
                post.get("caption"),
                post.get("username"),
                metrics["likeCount"],
                metrics["directReplyCount"],
                metrics["shares"],
                metrics["repostCount"],
                post.get("tagHeader"),
                now_taipei,
                permalink
            ))
            conn.commit()
            return "update"

        # INSERT
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
            post_time_taipei,
            post.get("keywordText"),
            post.get("caption"),
            permalink,
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
        return "insert"

    except Exception as e:
        conn.rollback()
        print("DB Error:", e)
        return "skip"

# =======================================================
# 手動匯入前 10 筆 + Email
# =======================================================
def manual_import_10():
    print("\n===== 🚀 手動匯入 10 筆貼文 =====")

    total = 0
    groups = get_keyword_groups()
    group_stats = {}

    for group in groups:
        group_name = group.get("groupName", "未知群組")
        posts = get_posts_by_group(group["id"])

        for p in posts:
            if total >= 10:
                break

            metrics = pick_best_metrics(get_metrics(p["code"]))
            result = upsert_post(p, metrics)

            if group_name not in group_stats:
                group_stats[group_name] = {"insert": 0, "update": 0, "total": 0}

            if result in ["insert", "update"]:
                group_stats[group_name][result] += 1
                group_stats[group_name]["total"] += 1

            total += 1

        if total >= 10:
            break

    # Build Email
    lines = ["【手動匯入前 10 筆】\n"]

    for g, stat in group_stats.items():
        lines.append(f"🔍 關鍵字群組：{g}")
        lines.append(f"📌 時段內貼文數：{stat['total']}")
        lines.append(f"🆕 新增：{stat['insert']}")
        lines.append(f"🔄 更新：{stat['update']}\n")

    send_email("Threads 手動匯入摘要", "\n".join(lines))
    print("📨 手動匯入 email 已寄出")

# =======================================================
# 每小時排程：抓前 3~2 小時 + Email
# =======================================================
def job_import_last_2_to_3_hours():
    print("\n===== ⏰ 每小時 Threads 匯入任務 =====")

    now = datetime.now(timezone.utc)
    start_time = now - timedelta(hours=3)
    end_time = now - timedelta(hours=2)

    groups = get_keyword_groups()
    lines = [f"時間區間（UTC）：{start_time} ～ {end_time}\n"]

    for group in groups:
        group_name = group.get("groupName", "未知群組")
        posts = get_posts_by_group(group["id"])

        stat = {"insert": 0, "update": 0, "total": 0}

        for p in posts:
            t = datetime.fromisoformat(p["postCreatedAt"].replace("Z", "+00:00"))
            if not (start_time <= t <= end_time):
                continue

            metrics = pick_best_metrics(get_metrics(p["code"]))
            result = upsert_post(p, metrics)

            if result in ["insert", "update"]:
                stat[result] += 1
                stat["total"] += 1

        if stat["total"] == 0:
            continue

        lines.append(f"🔍 關鍵字群組：{group_name}")
        lines.append(f"📌 時段內貼文數：{stat['total']}")
        lines.append(f"🆕 新增：{stat['insert']}")
        lines.append(f"🔄 更新：{stat['update']}\n")

    send_email("Threads 每小時匯入摘要", "\n".join(lines))
    print("📨 每小時 email 已寄出")

# =======================================================
# Flask + Scheduler
# =======================================================
app = Flask(__name__)
scheduler = BackgroundScheduler()

scheduler.add_job(job_import_last_2_to_3_hours, "cron", minute=0)
scheduler.add_job(manual_import_10, "date", run_date=datetime.utcnow() + timedelta(seconds=5))
scheduler.start()

@app.route("/health")
def health():
    return "OK", 200

@app.route("/")
def index():
    return "Threads Crawler Running"

if __name__ == "__main__":
    manual_import_10()
    app.run(host="0.0.0.0", port=5000)
