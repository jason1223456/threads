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
# Email 設定 (Gmail App Password)
# =======================================================
SMTP_USER = "jason91082500@gmail.com"
SMTP_PASS = "rwundvtaybzrgzlz"   # 請把你的 APP 密碼貼在這
EMAIL_TO = "leona@brainmax-marketing.com"

def send_email(subject, body):
    try:
        msg = MIMEText(body.encode("utf-8"), "plain", "utf-8")
        msg["Subject"] = Header(subject, "utf-8")
        msg["From"] = Header(SMTP_USER, "utf-8")
        msg["To"] = Header(EMAIL_TO, "utf-8")

        server = smtplib.SMTP_SSL("smtp.gmail.com", 465)
        server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(SMTP_USER, [EMAIL_TO], msg.as_string())
        server.quit()

        print("📧 Email 寄送成功")

    except Exception as e:
        print(f"❌ Email 寄送失敗： {e}")

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
    return r.json()["data"]   # groupName 在這裡

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
    try:
        cursor.execute("SELECT 1 FROM social_posts WHERE permalink=%s LIMIT 1", (permalink,))
        return cursor.fetchone()
    except:
        conn.rollback()
        return None

def upsert_post(post, metrics, group_name, result_list):
    try:
        post_time_utc = datetime.fromisoformat(post["postCreatedAt"].replace("Z", "+00:00"))
        post_time_taipei = (post_time_utc + TAIPEI_OFFSET).replace(tzinfo=None)
        now_taipei = (datetime.now(timezone.utc) + TAIPEI_OFFSET).replace(tzinfo=None)

        permalink = post["permalink"]
        existing = get_existing_post(permalink)

        record = {
            "group": group_name,
            "code": post["code"],
            "metrics": metrics
        }

        if existing:
            status = "更新"
            cursor.execute("""
                UPDATE social_posts
                SET 
                    keyword=%s,
                    content=%s,
                    poster_name=%s,
                    media_title='threads',
                    media_name='threads',
                    site='THREADS',
                    channel='threads專案',
                    api_source='threadslytics',
                    threads_like_count=%s,
                    threads_comment_count=%s,
                    threads_share_count=%s,
                    threads_repost_count=%s,
                    threads_topic=%s,
                    updated_at=%s
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

        else:
            status = "新增"
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
                    'threads', 'threads', 'THREADS', 'threads專案', 'threadslytics',
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

        record["status"] = status
        result_list.append(record)

    except Exception as e:
        print("❌ 寫入錯誤 — rollback")
        print(e)
        conn.rollback()

# =======================================================
# Email Format
# =======================================================
def format_email(group_name, records):
    body = f"📌 群組：{group_name}\n\n"
    for r in records:
        m = r["metrics"]
        body += (
            f"{'🆕' if r['status']=='新增' else '🔄'} {r['code']}\n"
            f"    👍 {m['likeCount']}   💬 {m['directReplyCount']}   "
            f"↗️ {m['shares']}   🔁 {m['repostCount']}\n\n"
        )
    return body

# =======================================================
# 手動匯入 10
# =======================================================
def manual_import_10():
    print("\n===== 🚀 手動匯入 10 筆貼文 =====")

    result_list = []
    total = 0

    for group in get_keyword_groups():
        group_name = group.get("groupName", "未知群組")
        posts = get_posts_by_group(group["id"])

        for p in posts:
            if total >= 10:
                email_body = format_email(group_name, result_list)
                send_email("手動匯入完成（前10筆）", email_body)
                print("\n🎉 已完成匯入 10 筆")
                return

            metrics = pick_best_metrics(get_metrics(p["code"]))
            upsert_post(p, metrics, group_name, result_list)
            total += 1

    # 所有都處理完才寄信
    email_body = format_email("全部群組", result_list)
    send_email("手動匯入完成（不到10筆）", email_body)

# =======================================================
# 每小時排程：抓前 3～2 小時貼文
# =======================================================
def job_import_last_2_to_3_hours():
    print("\n⏰ 每小時排程開始")

    now = datetime.now(timezone.utc)
    start_time = now - timedelta(hours=3)
    end_time = now - timedelta(hours=2)

    result_list = []

    for group in get_keyword_groups():
        group_name = group.get("groupName", "未知群組")
        for p in get_posts_by_group(group["id"]):
            t = datetime.fromisoformat(p["postCreatedAt"].replace("Z", "+00:00"))
            if start_time <= t <= end_time:
                metrics = pick_best_metrics(get_metrics(p["code"]))
                upsert_post(p, metrics, group_name, result_list)

    # 排程一次寄一封
    if result_list:
        send_email("每小時 Threads 更新通知", format_email("排程更新", result_list))

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
