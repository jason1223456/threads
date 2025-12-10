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
# ✉️ Gmail 寄信設定
# =======================================================

SMTP_USER = "jason91082500@gmail.com"
RAW_SMTP_PASS = "rwun dvta ybzr gzlz"  # 可以保留有空格的格式
# 把空格 & 不可見空白都移除，變成真正的 16 碼 app password
SMTP_PASS = RAW_SMTP_PASS.replace(" ", "").replace("\u00a0", "")

TO_EMAIL = "leona@brainmax-marketing.com"


def send_email(subject: str, body: str, to: str = TO_EMAIL):
    # 避免 \xa0 之類奇怪的空白造成編碼問題
    subject = (subject or "").replace("\xa0", " ")
    body = (body or "").replace("\xa0", " ")

    msg = MIMEMultipart()
    msg["From"] = SMTP_USER
    msg["To"] = to
    msg["Subject"] = subject

    # UTF-8 內容，避免 'ascii' codec 錯誤
    msg.attach(MIMEText(body, "plain", "utf-8"))

    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.ehlo()
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(SMTP_USER, [to], msg.as_string())
        server.quit()
        print(f"📧 寄信成功 → {subject}")
    except Exception as e:
        print(f"❌ Email 寄送失敗：{e}")


def send_summary_email(title: str, added: list, updated: list, failed: list):
    """統一寄出一次摘要 Email"""
    subject = f"📊 Threads 匯入摘要：{title}"

    def fmt_list(lst):
        return "\n".join(lst) if lst else "（無）"

    body = f"""
【Threads 匯入摘要 — {title}】

🆕 新增成功：{len(added)} 筆
🔄 更新成功：{len(updated)} 筆
❌ 失敗：{len(failed)} 筆

---------------------------------------
🆕 新增清單（code）：
{fmt_list(added)}

---------------------------------------
🔄 更新清單（code）：
{fmt_list(updated)}

---------------------------------------
❌ 失敗清單（code）：
{fmt_list(failed)}

---------------------------------------
時間：{datetime.now()}
"""

    send_email(subject, body)


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
        cursor.execute(
            "SELECT 1 FROM social_posts WHERE permalink=%s LIMIT 1",
            (permalink,)
        )
        return cursor.fetchone()
    except Exception as e:
        print("❌ 查 existing_post 錯誤：", e)
        conn.rollback()
        return None


def upsert_post(post, metrics):
    """
    處理單一貼文：
    回傳 dict:
      {"status": "insert"/"update"/"fail", "code": ..., "error": ...}
    """
    result = {"status": "", "code": post.get("code")}

    try:
        post_time_utc = datetime.fromisoformat(
            post["postCreatedAt"].replace("Z", "+00:00")
        )
        post_time_taipei = (post_time_utc + TAIPEI_OFFSET).replace(tzinfo=None)
        now_taipei = (datetime.now(timezone.utc) + TAIPEI_OFFSET).replace(tzinfo=None)

        permalink = post["permalink"]
        existing = get_existing_post(permalink)

        if existing:
            # ===== UPDATE =====
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
            result["status"] = "update"
            print(f"🔄 更新：{post['code']}")

        else:
            # ===== INSERT =====
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
            result["status"] = "insert"
            print(f"🆕 新增：{post['code']}")

        conn.commit()
        return result

    except Exception as e:
        print("❌ 寫入錯誤 — rollback", e)
        conn.rollback()
        result["status"] = "fail"
        result["error"] = str(e)
        return result


# =======================================================
# 手動匯入前 10 筆，結束寄一封摘要信
# =======================================================
def manual_import_10():
    print("\n===== 🚀 手動匯入 10 筆貼文 =====")

    added = []
    updated = []
    failed = []

    total = 0

    for group in get_keyword_groups():
        posts = get_posts_by_group(group["id"])

        for p in posts:
            if total >= 10:
                break

            metrics = pick_best_metrics(get_metrics(p["code"]))
            result = upsert_post(p, metrics)

            if result["status"] == "insert":
                added.append(result["code"])
            elif result["status"] == "update":
                updated.append(result["code"])
            else:
                failed.append(result["code"])

            total += 1
            print(f"🆕 第 {total} 筆：{p['code']}")

        if total >= 10:
            break

    print("🎉 手動匯入完成")

    # 匯總寄信
    send_summary_email("手動匯入 10 筆", added, updated, failed)


# =======================================================
# 每小時抓前 3～2 小時，結束寄一封摘要信
# =======================================================
def job_import_last_2_to_3_hours():
    print("\n⏰ 定時任務：抓前 3～2 小時貼文")

    added = []
    updated = []
    failed = []

    now = datetime.now(timezone.utc)
    start_time = now - timedelta(hours=3)
    end_time = now - timedelta(hours=2)

    for group in get_keyword_groups():
        posts = get_posts_by_group(group["id"])

        for p in posts:
            t = datetime.fromisoformat(p["postCreatedAt"].replace("Z", "+00:00"))

            if start_time <= t <= end_time:
                metrics = pick_best_metrics(get_metrics(p["code"]))
                result = upsert_post(p, metrics)

                if result["status"] == "insert":
                    added.append(result["code"])
                elif result["status"] == "update":
                    updated.append(result["code"])
                else:
                    failed.append(result["code"])

    print(f"✨ 本次排程新增 {len(added)}，更新 {len(updated)}，失敗 {len(failed)}")
    send_summary_email("每小時更新（前 3～2 小時）", added, updated, failed)


# =======================================================
# Flask + Scheduler
# =======================================================
app = Flask(__name__)
scheduler = BackgroundScheduler()

# 每小時整點跑一次
scheduler.add_job(job_import_last_2_to_3_hours, "cron", minute=0)

# 啟動後 5 秒先跑一次手動 10 筆（方便測試）
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
