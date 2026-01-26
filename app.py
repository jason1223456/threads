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
REQ_TIMEOUT = 60  # ✅ 避免 ReadTimeout，拉長一點

# 時區設定
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
SMTP_PASS = "rwundvtaybzrgzlz"
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
    r = requests.get(f"{API_DOMAIN}/keyword-groups", headers=HEADERS, timeout=REQ_TIMEOUT)
    r.raise_for_status()
    return r.json()["data"]

def get_posts_by_group(group_id):
    posts = []
    page = 1

    while True:
        r = requests.get(
            f"{API_DOMAIN}/keyword-groups/analytics/{group_id}",
            headers=HEADERS,
            params={"metricDays": 7, "page": page},
            timeout=REQ_TIMEOUT
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
        params={"code": code},
        timeout=REQ_TIMEOUT
    )
    r.raise_for_status()
    return r.json().get("data", [])

# =======================================================
# METRICS
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
# DB FUNCTIONS (social_posts 原本貼文表)
# =======================================================
def get_existing_post(permalink):
    try:
        cursor.execute("SELECT 1 FROM social_posts WHERE permalink=%s LIMIT 1", (permalink,))
        return cursor.fetchone()
    except:
        conn.rollback()
        return None

def upsert_post(post, metrics):
    """
    保留原本 social_posts 行為：permalink 唯一，一篇貼文只存一筆
    """
    try:
        post_utc = datetime.fromisoformat(post["postCreatedAt"].replace("Z", "+00:00"))
        post_tw = (post_utc + TAIPEI_OFFSET).replace(tzinfo=None)
        now_tw = (datetime.utcnow() + TAIPEI_OFFSET).replace(tzinfo=None)

        permalink = post["permalink"]
        exists = get_existing_post(permalink)

        if exists:  # UPDATE
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
                post.get("keywordText"), post.get("caption"), post.get("username"),
                metrics["likeCount"], metrics["directReplyCount"],
                metrics["shares"], metrics["repostCount"],
                post.get("tagHeader"), now_tw, permalink
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
            post_tw, post.get("keywordText"), post.get("caption"),
            permalink, post.get("username"),
            metrics["likeCount"], metrics["directReplyCount"],
            metrics["shares"], metrics["repostCount"],
            post.get("tagHeader"),
            now_tw, now_tw
        ))
        conn.commit()
        return "insert"

    except Exception as e:
        print("DB Error (social_posts):", e)
        conn.rollback()
        return "skip"

# =======================================================
# DB FUNCTIONS (social_posts_events 事件表) ✅ 新增
# =======================================================
def upsert_event(post, group_name, metrics):
    """
    social_posts_events：一筆 = 一次命中事件（permalink + group + keyword）
    你要 2000+ 就靠這張表
    """
    try:
        post_utc = datetime.fromisoformat(post["postCreatedAt"].replace("Z", "+00:00"))
        post_tw = (post_utc + TAIPEI_OFFSET).replace(tzinfo=None)
        now_tw = (datetime.utcnow() + TAIPEI_OFFSET).replace(tzinfo=None)

        permalink = post["permalink"]
        keyword_text = post.get("keywordText")
        code = post.get("code")

        cursor.execute("""
            INSERT INTO social_posts_events (
                post_time, permalink, code,
                keyword_group, keyword,
                poster_name, content, threads_topic,
                threads_like_count, threads_comment_count,
                threads_share_count, threads_repost_count,
                site, channel, api_source,
                created_at, updated_at
            )
            VALUES (
                %s, %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s,
                'THREADS', 'threads專案', 'threadslytics',
                %s, %s
            )
            ON CONFLICT (permalink, keyword_group, keyword)
            DO UPDATE SET
                poster_name = EXCLUDED.poster_name,
                content = EXCLUDED.content,
                threads_topic = EXCLUDED.threads_topic,
                threads_like_count = EXCLUDED.threads_like_count,
                threads_comment_count = EXCLUDED.threads_comment_count,
                threads_share_count = EXCLUDED.threads_share_count,
                threads_repost_count = EXCLUDED.threads_repost_count,
                updated_at = EXCLUDED.updated_at
        """, (
            post_tw, permalink, code,
            group_name, keyword_text,
            post.get("username"), post.get("caption"), post.get("tagHeader"),
            metrics["likeCount"], metrics["directReplyCount"],
            metrics["shares"], metrics["repostCount"],
            now_tw, now_tw
        ))

        conn.commit()
        return "event_upsert"

    except Exception as e:
        print("DB Error (social_posts_events):", e)
        conn.rollback()
        return "skip"

# =======================================================
# 手動匯入（前 10 筆）
# =======================================================
def manual_import_10():
    print("\n===== 🚀 手動匯入 10 筆 =====")
    total = 0
    groups = get_keyword_groups()
    stats = {}

    for group in groups:
        gname = group.get("groupName", "未知群組")
        posts = get_posts_by_group(group["id"])

        for p in posts:
            if total >= 10:
                break

            metrics = pick_best_metrics(get_metrics(p["code"]))

            # ✅ 原本表：貼文表（不破壞其他匯入）
            result = upsert_post(p, metrics)
            # ✅ 新增表：事件表（你要 2000+ 靠這裡）
            upsert_event(p, gname, metrics)

            if gname not in stats:
                stats[gname] = {"insert": 0, "update": 0, "total": 0}

            if result in ["insert", "update"]:
                stats[gname][result] += 1
                stats[gname]["total"] += 1

            total += 1

        if total >= 10:
            break

    # ===== Email =====
    lines = ["【手動匯入前 10 筆】\n"]
    for g, s in stats.items():
        lines.append(f"🔍 關鍵字群組：{g}")
        lines.append(f"📌 時段內貼文數：{s['total']}")
        lines.append(f"🆕 新增：{s['insert']}")
        lines.append(f"🔄 更新：{s['update']}\n")

    send_email("Threads 手動匯入摘要", "\n".join(lines))

# =======================================================
# 每小時排程（前 3～2 小時）
# =======================================================
def job_import_last_2_to_3_hours():
    print("\n===== ⏰ 每小時 Threads 匯入 =====")

    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=3)
    end = now - timedelta(hours=2)

    # ⭐ 台北時間（Email 顯示用）
    start_tw = (start + TAIPEI_OFFSET).replace(tzinfo=None)
    end_tw = (end + TAIPEI_OFFSET).replace(tzinfo=None)

    lines = [
        f"時間區間：{start_tw.strftime('%Y-%m-%d %H:%M:%S')} ～ {end_tw.strftime('%Y-%m-%d %H:%M:%S')}\n"
    ]

    groups = get_keyword_groups()

    for group in groups:
        gname = group.get("groupName", "未知群組")
        posts = get_posts_by_group(group["id"])

        stat = {"insert": 0, "update": 0, "total": 0}

        for p in posts:
            t = datetime.fromisoformat(p["postCreatedAt"].replace("Z", "+00:00"))
            if not (start <= t <= end):
                continue

            metrics = pick_best_metrics(get_metrics(p["code"]))

            # ✅ 原本表
            result = upsert_post(p, metrics)
            # ✅ 事件表（同時寫入）
            upsert_event(p, gname, metrics)

            if result in ["insert", "update"]:
                stat[result] += 1
                stat["total"] += 1

        if stat["total"] == 0:
            continue

        lines.append(f"🔍 關鍵字群組：{gname}")
        lines.append(f"📌 時段內貼文數：{stat['total']}")
        lines.append(f"🆕 新增：{stat['insert']}")
        lines.append(f"🔄 更新：{stat['update']}\n")

    send_email("Threads 每小時匯入摘要", "\n".join(lines))

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
