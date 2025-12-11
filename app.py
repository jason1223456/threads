import requests
import psycopg
from psycopg.rows import dict_row
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta, timezone
import smtplib
from email.mime.text import MIMEText
from email.header import Header
import os

# =======================================================
# API TOKEN
# =======================================================
API_TOKEN = "bscU4YK22+OYofSoh105OuVJZAh4tsYWZhKawi7WKjY="
API_DOMAIN = "https://api.threadslytics.com/v1"
HEADERS = {"Authorization": f"Bearer {API_TOKEN}"}
TAIPEI_OFFSET = timedelta(hours=8)

# =======================================================
# EMAIL SETTING (建議放環境變數)
# =======================================================
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USER = "jason91082500@gmail.com"   # 例如：myaccount@gmail.com
SMTP_PASS = "rwun dvta ybzr gzlz"   
EMAIL_TO = "leona@brainmax-marketing.com"

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
# EMAIL FUNCTION
# =======================================================
def send_email(subject, body):
    """寄信（UTF-8 避免 ASCII Error）"""
    try:
        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = Header(subject, "utf-8")
        msg["From"] = SMTP_USER
        msg["To"] = EMAIL_TO

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as s:
            s.starttls()
            s.login(SMTP_USER, SMTP_PASS)
            s.send_message(msg)

        print("📧 Email 已寄送")

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
# DB FUNCTIONS
# =======================================================
def get_existing_post(permalink):
    cursor.execute("SELECT 1 FROM social_posts WHERE permalink=%s LIMIT 1", (permalink,))
    return cursor.fetchone()

def upsert_post(post, metrics, group_name, summary_list):
    """寫入 + 收集 email summary"""
    try:
        post_time_utc = datetime.fromisoformat(post["postCreatedAt"].replace("Z", "+00:00"))
        post_time_taipei = (post_time_utc + TAIPEI_OFFSET).replace(tzinfo=None)
        now_taipei = (datetime.now(timezone.utc) + TAIPEI_OFFSET).replace(tzinfo=None)
        permalink = post["permalink"]

        existing = get_existing_post(permalink)

        status = "更新" if existing else "新增"

        # INSERT
        if not existing:
            cursor.execute("""
                INSERT INTO social_posts (
                    date, keyword, content, permalink, poster_name,
                    media_title, media_name, site, channel, api_source,
                    threads_like_count, threads_comment_count,
                    threads_share_count, threads_repost_count,
                    threads_topic, created_at, updated_at
                )
                VALUES (
                    %s,%s,%s,%s,%s,
                    'threads','threads','THREADS','threads專案','threadslytics',
                    %s,%s,%s,%s,
                    %s,%s,%s
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

        # UPDATE
        else:
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

        # 收集寄信內容
        summary_list.append({
            "status": status,
            "code": post["code"],
            "metrics": metrics,
            "groupName": group_name
        })

    except Exception as e:
        print("❌ 寫入錯誤 — rollback", e)
        conn.rollback()

# =======================================================
# EMAIL SUMMARY FORMAT
# =======================================================
def format_summary_email(summary_list):
    if not summary_list:
        return "沒有新增或更新資料。"

    output = ""

    # 依群組分類
    groups = {}
    for item in summary_list:
        groups.setdefault(item["groupName"], []).append(item)

    for group, items in groups.items():
        output += f"📌 群組：{group}\n\n"
        for i in items:
            output += (
                f"{'🆕' if i['status']=='新增' else '🔄'} {i['code']}\n"
                f"    👍 {i['metrics']['likeCount']}   "
                f"💬 {i['metrics']['directReplyCount']}   "
                f"↗️ {i['metrics']['shares']}   "
                f"🔁 {i['metrics']['repostCount']}\n\n"
            )
        output += "\n-------------------------\n\n"

    return output

# =======================================================
# 手動匯入前 10
# =======================================================
def manual_import_10():
    print("\n===== 🚀 手動匯入 10 筆貼文 → social_posts =====")
    
    summary_list = []
    count = 0

    for group in get_keyword_groups():
        group_name = group.get("groupName", "未命名群組")

        for p in get_posts_by_group(group["id"]):
            if count >= 10:
                break

            metrics = pick_best_metrics(get_metrics(p["code"]))
            upsert_post(p, metrics, group_name, summary_list)

            count += 1

    # 寄信
    email_body = format_summary_email(summary_list)
    send_email("Threads 手動匯入結果通知", email_body)

# =======================================================
# 每小時排程：抓前 3~2 小時
# =======================================================
def job_import_last_2_to_3_hours():
    print("\n⏰ 定時任務：抓前 3～2 小時貼文 → social_posts")

    now = datetime.now(timezone.utc)
    start_time = now - timedelta(hours=3)
    end_time = now - timedelta(hours=2)

    summary_list = []

    for group in get_keyword_groups():
        group_name = group.get("groupName", "未命名群組")

        for p in get_posts_by_group(group["id"]):
            t = datetime.fromisoformat(p["postCreatedAt"].replace("Z", "+00:00"))
            if start_time <= t <= end_time:
                metrics = pick_best_metrics(get_metrics(p["code"]))
                upsert_post(p, metrics, group_name, summary_list)

    # 排程也寄信
    email_body = format_summary_email(summary_list)
    send_email("Threads 每小時更新通知", email_body)

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
    return "Threads SocialPosts Crawler Running"

if __name__ == "__main__":
    manual_import_10()
    app.run(host="0.0.0.0", port=5000)
