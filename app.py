import requests
import psycopg
from psycopg.rows import dict_row
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta, timezone

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# =======================================================
# ✉️ Gmail 寄信設定（直接填寫）
# =======================================================
SMTP_USER = "jason91082500@gmail.com"   # 例如：myaccount@gmail.com
SMTP_PASS = "rwun dvta ybzr gzlz"          # ← 改這行
TO_EMAIL  = "leona@brainmax-marketing.com"

def send_email(subject: str, body: str, to: str = TO_EMAIL):
    msg = MIMEMultipart()
    msg["From"] = SMTP_USER
    msg["To"] = to
    msg["Subject"] = subject

    # 用 UTF-8 避免 ascii encode 錯誤
    msg.attach(MIMEText(body, "plain", "utf-8"))

    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(SMTP_USER, [to], msg.as_string())
        server.quit()
        print(f"📧 Email 已寄出 → {subject}")
    except Exception as e:
        print("❌ Email 寄送失敗：", e)


# =======================================================
# API 設定
# =======================================================
API_TOKEN = "bscU4YK22+OYofSoh105OuVJZAh4tsYWZhKawi7WKjY="
API_DOMAIN = "https://api.threadslytics.com/v1"
HEADERS = {"Authorization": f"Bearer {API_TOKEN}"}
TAIPEI_OFFSET = timedelta(hours=8)

# =======================================================
# PostgreSQL
# =======================================================
DATABASE_URL = (
    "postgresql://root:L2em9nY8K4PcxCuXV60tf1Hs5MG7j3Oz"
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
        if any([nm["likeCount"], nm["directReplyCount"], nm["shares"], nm["repostCount"]]):
            return nm
    return normalize_metrics(metrics[0])


# =======================================================
# DB FUNCTIONS — channel = 'threads專案' + api_source='threadslytics'
# =======================================================
def get_existing_post(permalink):
    try:
        cursor.execute("SELECT 1 FROM social_posts WHERE permalink=%s LIMIT 1", (permalink,))
        return cursor.fetchone()
    except Exception:
        conn.rollback()
        return None


def upsert_post(post, metrics):
    status = ""  # 用來記錄匯入成功 / 更新 / 失敗
    try:
        post_time_utc = datetime.fromisoformat(post["postCreatedAt"].replace("Z", "+00:00"))
        post_time_taipei = (post_time_utc + TAIPEI_OFFSET).replace(tzinfo=None)
        now_taipei = (datetime.now(timezone.utc) + TAIPEI_OFFSET).replace(tzinfo=None)

        permalink = post["permalink"]
        existing = get_existing_post(permalink)

        # ================= UPDATE =================
        if existing:
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
            status = f"更新：{post['code']}"

        # ================= INSERT =================
        else:
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
            status = f"新增：{post['code']}"

        conn.commit()
        print("🟢", status)
        send_email("Threads 匯入成功", status)

    except Exception as e:
        error_msg = f"❌ 匯入失敗：{post.get('code')} → {e}"
        print(error_msg)
        conn.rollback()
        send_email("❌ Threads 匯入失敗", error_msg)


# =======================================================
# 手動匯入前 10
# =======================================================
def manual_import_10():
    print("\n===== 🚀 手動匯入 10 筆貼文 =====")
    total = 0
    for group in get_keyword_groups():
        for p in get_posts_by_group(group["id"]):
            if total >= 10:
                print("🎉 匯入完成 10 筆")
                return
            metrics = pick_best_metrics(get_metrics(p["code"]))
            upsert_post(p, metrics)
            total += 1
            print(f"🆕 第 {total} 筆：{p['code']}")


# =======================================================
# 每小時抓前 3~2 小時
# =======================================================
def job_import_last_2_to_3_hours():
    print("\n⏰ 執行定時匯入（前 3～2 小時）")
    now = datetime.now(timezone.utc)
    start_time = now - timedelta(hours=3)
    end_time   = now - timedelta(hours=2)
    total = 0

    for group in get_keyword_groups():
        for p in get_posts_by_group(group["id"]):
            t = datetime.fromisoformat(p["postCreatedAt"].replace("Z", "+00:00"))
            if start_time <= t <= end_time:
                metrics = pick_best_metrics(get_metrics(p["code"]))
                upsert_post(p, metrics)
                total += 1

    send_email("⏰ 定時匯入完成", f"本次共匯入：{total} 筆")


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
