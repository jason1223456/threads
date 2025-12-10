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
# ✉️ Email 設定
# =======================================================
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USER = "jason91082500@gmail.com"

# 這裡保留你原本看到有空格的格式，程式自動去空白
RAW_SMTP_PASS = "rwun dvta ybzr gzlz"
SMTP_PASS = RAW_SMTP_PASS.replace(" ", "").replace("\u00a0", "")

REPORT_RECEIVER = "leona@brainmax-marketing.com"


def send_email(subject: str, body: str):
    """寄出純文字 Email（UTF-8）"""
    subject = (subject or "").replace("\xa0", " ")
    body = (body or "").replace("\xa0", " ")

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

        print(f"📧 Email 寄送成功：{subject}")
    except Exception as e:
        print("❌ Email 寄送失敗：", e)


# =======================================================
# Threadslytics API 設定
# =======================================================
API_TOKEN = "bscU4YK22+OYofSoh105OuVJZAh4tsYWZhKawi7WKjY="
API_DOMAIN = "https://api.threadslytics.com/v1"
HEADERS = {"Authorization": f"Bearer {API_TOKEN}"}

TAIPEI_OFFSET = timedelta(hours=8)

# =======================================================
# PostgreSQL 設定
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
# METRICS 正規化
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
# DB FUNCTIONS — channel='threads專案' + api_source='threadslytics'
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
    處理單一貼文，回傳：
      'insert' / 'update' / 'fail'
    """
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
            conn.commit()
            return "update"

        # ===== INSERT =====
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
        conn.commit()
        return "insert"

    except Exception as e:
        print("❌ 寫入錯誤 — rollback：", e)
        conn.rollback()
        return "fail"


# =======================================================
# 手動匯入前 10 筆（結束寄一封摘要信，按 groupName 統計）
# =======================================================
def manual_import_10():
    print("\n===== 🚀 手動匯入 10 筆貼文 → social_posts =====")

    # groupName -> {'insert': x, 'update': y, 'total': z}
    group_stats = {}
    total = 0

    groups = get_keyword_groups()

    for group in groups:
        group_name = group.get("groupName", "未知群組")  # ⭐ 用 groupName
        posts = get_posts_by_group(group["id"])

        for p in posts:
            if total >= 10:
                break

            metrics = pick_best_metrics(get_metrics(p["code"]))
            result = upsert_post(p, metrics)

            if group_name not in group_stats:
                group_stats[group_name] = {"insert": 0, "update": 0, "total": 0}

            if result == "insert":
                group_stats[group_name]["insert"] += 1
            elif result == "update":
                group_stats[group_name]["update"] += 1

            group_stats[group_name]["total"] += 1
            total += 1

            print(f"✅ 第 {total} 筆：{p['code']}（群組：{group_name}，結果：{result}）")

        if total >= 10:
            break

    # 組 Email 內容
    lines = []
    lines.append("【手動匯入前 10 筆貼文結果】\n")

    if not group_stats:
        lines.append("本次沒有任何貼文被處理。")
    else:
        for name, stat in group_stats.items():
            lines.append(f"🔍 關鍵字群組：{name}")
            lines.append(f"  📌 總處理：{stat['total']} 筆")
            lines.append(f"  🆕 新增：{stat['insert']} 筆")
            lines.append(f"  🔄 更新：{stat['update']} 筆\n")

    body = "\n".join(lines)
    send_email("Threads 手動匯入前 10 筆摘要（依關鍵字群組）", body)

    print("\n🎉 手動匯入完成，已寄出摘要 email")


# =======================================================
# 每小時：抓前 3～2 小時的貼文（依 groupName 統計）
# =======================================================
def job_import_last_2_to_3_hours():
    print("\n===== ⏰ 每小時 Threads 匯入任務開始 =====")

    now = datetime.now(timezone.utc)
    start_time = now - timedelta(hours=3)
    end_time = now - timedelta(hours=2)

    groups = get_keyword_groups()

    lines = []
    lines.append(f"時間區間（UTC）：{start_time} ～ {end_time}\n")

    for group in groups:
        group_name = group.get("groupName", "未知群組")  # ⭐ 用 groupName
        posts = get_posts_by_group(group["id"])

        group_insert = 0
        group_update = 0
        group_total = 0

        # 篩選時間區間內的貼文
        filtered = []
        for p in posts:
            t = datetime.fromisoformat(p["postCreatedAt"].replace("Z", "+00:00"))
            if start_time <= t <= end_time:
                filtered.append(p)

        # 沒有貼文
        if not filtered:
            lines.append(f"🔍 關鍵字群組：{group_name}")
            lines.append("  ⚠️ 這個時間區間內沒有貼文，不寫入資料庫\n")
            continue

        # 有貼文 → 寫入
        for p in filtered:
            metrics = pick_best_metrics(get_metrics(p["code"]))
            result = upsert_post(p, metrics)

            if result == "insert":
                group_insert += 1
            elif result == "update":
                group_update += 1

            group_total += 1

        lines.append(f"🔍 關鍵字群組：{group_name}")
        lines.append(f"  📌 時段內貼文數：{group_total}")
        lines.append(f"  🆕 新增：{group_insert}")
        lines.append(f"  🔄 更新：{group_update}\n")

    body = "\n".join(lines)
    send_email("Threads 每小時匯入摘要（依關鍵字群組）", body)

    print("🎉 每小時任務完成，已寄出摘要 email")


# =======================================================
# Flask + 排程
# =======================================================
app = Flask(__name__)
scheduler = BackgroundScheduler()

# 每小時整點跑一次
scheduler.add_job(job_import_last_2_to_3_hours, "cron", minute=0)

# 啟動後 5 秒先手動匯入 10 筆（方便測試）
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
    # 本地執行時也會先跑一次手動 10 筆
    manual_import_10()
    app.run(host="0.0.0.0", port=5000)
