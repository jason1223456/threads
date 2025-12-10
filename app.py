# ========================== IMPORTS ==========================
import requests
import psycopg
from psycopg.rows import dict_row
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta, timezone
import smtplib
from email.mime.text import MIMEText
from email.header import Header

# ========================== EMAIL CONFIG ==========================
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USER = "jason91082500@gmail.com"
SMTP_PASS = "rwundvtaybzrgzlz"  # ← 去掉空白，必須是 *連續字串*

REPORT_RECEIVER = "leona@brainmax-marketing.com"

# ========================== API CONFIG ==========================
API_TOKEN = "bscU4YK22+OYofSoh105OuVJZAh4tsYWZhKawi7WKjY="
API_DOMAIN = "https://api.threadslytics.com/v1"
HEADERS = {"Authorization": f"Bearer {API_TOKEN}"}

TAIPEI_OFFSET = timedelta(hours=8)

# ========================== DATABASE ==========================
DATABASE_URL = (
    "postgresql://root:"
    "L2em9nY8K4PcxCuXV60tf1Hs5MG7j3Oz"
    "@sfo1.clusters.zeabur.com:30599/zeabur"
)
conn = psycopg.connect(DATABASE_URL, row_factory=dict_row)
cursor = conn.cursor()

# ========================== EMAIL SENDER ==========================
def send_email(subject, body):
    try:
        msg = MIMEText(body, "plain", "utf-8")
        msg["From"] = SMTP_USER
        msg["To"] = REPORT_RECEIVER
        msg["Subject"] = Header(subject, "utf-8")

        smtp = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        smtp.starttls()
        smtp.login(SMTP_USER, SMTP_PASS)
        smtp.send_message(msg)
        smtp.quit()

        print("📧 Email 寄送成功！")
    except Exception as e:
        print("❌ Email 寄送失敗：", e)

# ========================== API FUNCTIONS ==========================
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

# ========================== METRICS CLEAN ==========================
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

# ========================== DB FUNCTIONS ==========================
def get_existing_post(permalink):
    cursor.execute("SELECT 1 FROM social_posts WHERE permalink=%s LIMIT 1", (permalink,))
    return cursor.fetchone()

def upsert_post(post, metrics):
    post_time_utc = datetime.fromisoformat(post["postCreatedAt"].replace("Z", "+00:00"))
    post_time_taipei = (post_time_utc + TAIPEI_OFFSET).replace(tzinfo=None)
    now_taipei = (datetime.now(timezone.utc) + TAIPEI_OFFSET).replace(tzinfo=None)
    permalink = post["permalink"]

    existing = get_existing_post(permalink)

    # UPDATE
    if existing:
        cursor.execute("""
            UPDATE social_posts
            SET keyword=%s,
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
        VALUES (%s,%s,%s,%s,%s,
                'threads','threads','THREADS','threads專案','threadslytics',
                %s,%s,%s,%s,
                %s,%s,%s)
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
    return "insert"

# ========================== MAIN JOB ==========================
def job_import_last_2_to_3_hours():
    print("\n===== ⏰ Threads 定時任務開始 =====")

    now = datetime.now(timezone.utc)
    start_time = now - timedelta(hours=3)
    end_time = now - timedelta(hours=2)

    report_lines = []
    grand_total = 0

    groups = get_keyword_groups()

    for group in groups:
        group_name = group["name"]
        posts = get_posts_by_group(group["id"])

        group_insert = 0
        group_update = 0

        filtered_posts = []
        for p in posts:
            t = datetime.fromisoformat(p["postCreatedAt"].replace("Z", "+00:00"))
            if start_time <= t <= end_time:
                filtered_posts.append(p)

        # 沒有貼文
        if not filtered_posts:
            report_lines.append(f"🔍 關鍵字群組：{group_name}\n⚠️ 最近 1 小時內無貼文，不寫入資料庫\n")
            continue

        # 有貼文 → 處理
        for p in filtered_posts:
            metrics = pick_best_metrics(get_metrics(p["code"]))
            result = upsert_post(p, metrics)

            if result == "insert":
                group_insert += 1
            elif result == "update":
                group_update += 1

            grand_total += 1
            conn.commit()

        # 群組總結
        summary = f"🔍 關鍵字群組：{group_name}\n"
        summary += f"🆕 新增：{group_insert}\n"
        summary += f"🔄 更新：{group_update}\n"
        report_lines.append(summary + "\n")

    # ================== 寄出整體結果 ==================
    email_subject = "Threadslytics 每小時更新報告"
    email_body = "\n".join(report_lines)

    send_email(email_subject, email_body)
    print(email_body)

# ========================== FLASK SERVER ==========================
app = Flask(__name__)
scheduler = BackgroundScheduler()

scheduler.add_job(job_import_last_2_to_3_hours, "cron", minute=0)
scheduler.start()

@app.route("/health")
def health():
    return "OK", 200

if __name__ == "__main__":
    job_import_last_2_to_3_hours()
    app.run(host="0.0.0.0", port=5000)
