import requests
import sqlite3
import time
import json
import csv
from datetime import datetime, timedelta

# All games from Watch Dogs franchise to pull reviews from
GAMES = [
    {
        "title": "Watch_Dogs",
        "slug": "watch_dogs",
        "appid": 243470,
        "release_date": "2014-05-27",   # YYYY-MM-DD
    },
    {
        "title": "Watch_Dogs 2",
        "slug": "watch_dogs_2",
        "appid": 447040,
        "release_date": "2016-11-28",
    },
    {
        "title": "Watch Dogs: Legion",
        "slug": "watch_dogs_legion",
        "appid": 2239550,
        "release_date": "2020-10-29",
    },
]


def init_db(db_path: str):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS reviews (
            recommendationid TEXT PRIMARY KEY,
            steamid TEXT,
            review TEXT,
            timestamp_created INTEGER,
            timestamp_updated INTEGER,
            voted_up INTEGER,
            weighted_vote_score REAL,
            playtime_forever INTEGER,
            playtime_at_review INTEGER,
            last_played INTEGER,
            raw_json TEXT
        )
    """)
    conn.commit()
    return conn


def init_csv(csv_path: str):
    f = open(csv_path, mode="w", newline="", encoding="utf-8")
    writer = csv.writer(f)
    writer.writerow([
        "recommendationid",
        "steamid",
        "review",
        "timestamp_created",
        "timestamp_updated",
        "voted_up",
        "weighted_vote_score",
        "playtime_forever",
        "playtime_at_review",
        "last_played",
        "raw_json",
    ])
    return f, writer


def save_review_db(conn, r):
    cur = conn.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO reviews (
            recommendationid,
            steamid,
            review,
            timestamp_created,
            timestamp_updated,
            voted_up,
            weighted_vote_score,
            playtime_forever,
            playtime_at_review,
            last_played,
            raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        r.get("recommendationid"),
        str(r.get("author", {}).get("steamid")),
        r.get("review"),
        r.get("timestamp_created"),
        r.get("timestamp_updated"),
        1 if r.get("voted_up") else 0,
        float(r.get("weighted_vote_score") or 0.0),
        r.get("author", {}).get("playtime_forever"),
        r.get("author", {}).get("playtime_at_review"),
        r.get("author", {}).get("last_played"),
        json.dumps(r, ensure_ascii=False)
    ))
    conn.commit()


def save_review_csv(writer, r):
    writer.writerow([
        r.get("recommendationid"),
        str(r.get("author", {}).get("steamid")),
        r.get("review"),
        r.get("timestamp_created"),
        r.get("timestamp_updated"),
        1 if r.get("voted_up") else 0,
        float(r.get("weighted_vote_score") or 0.0),
        r.get("author", {}).get("playtime_forever"),
        r.get("author", {}).get("playtime_at_review"),
        r.get("author", {}).get("last_played"),
        json.dumps(r, ensure_ascii=False),
    ])


def fetch_all_reviews(app_id, conn, csv_writer):
    steam_reviews_url = f"https://store.steampowered.com/appreviews/{app_id}"
    cursor = "*"
    total = 0
    unique = set()

    while len(unique) < 40000: #Want to avoid having too large of a file size:
        params = {
            "json": 1,
            "language": "all",  # we can change to english if we perform any sentiment analysis on it
            "filter": "recent",
            "review_type": "all",
            "purchase_type": "all",
            "num_per_page": 100,
            "cursor": cursor,
        }

        resp = requests.get(steam_reviews_url, params=params, timeout=5)

        if resp.status_code == 429:
            print(f"[appid {app_id}] rate limited, sleeping 10s...")
            time.sleep(10)
            continue

        resp.raise_for_status()
        data = resp.json()

        if data.get("success") != 1:
            print(f"[appid {app_id}] request unsuccessful")
            break

        reviews = data.get("reviews", [])
        if not reviews:
            print(f"[appid {app_id}] Finished fetching all reviews (no more pages).")
            break

        for r in reviews:
            rec_id = r.get("recommendationid")
            if rec_id not in unique:
                unique.add(rec_id)
                save_review_db(conn, r)
                save_review_csv(csv_writer, r)
                total += 1


        new_cursor = data.get("cursor")
        if not new_cursor or new_cursor == cursor:
            print(f"[appid {app_id}] cursor did not advance, stopping.")
            break

        cursor = new_cursor

    print(f"[appid {app_id}] Done. Total unique reviews saved: {len(unique)}")



def export_first_90_days_csv(db_path: str, out_csv_path: str, release_date_str: str):
    """
    Read from the SQLite DB for one game and write a CSV containing
    only reviews within 90 days of release_date_str (YYYY-MM-DD).
    """
    # Compute unix timestamps for [release_date, release_date + 90 days]
    release_dt = datetime.strptime(release_date_str, "%Y-%m-%d")
    end_dt = release_dt + timedelta(days=90)
    release_ts = int(release_dt.timestamp())
    end_ts = int(end_dt.timestamp())

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("""
        SELECT
            recommendationid,
            steamid,
            review,
            timestamp_created,
            timestamp_updated,
            voted_up,
            weighted_vote_score,
            playtime_forever,
            playtime_at_review,
            last_played,
            raw_json
        FROM reviews
        WHERE timestamp_created BETWEEN ? AND ?
        ORDER BY timestamp_created ASC
    """, (release_ts, end_ts))

    rows = cur.fetchall()
    conn.close()

    # Write filtered rows to a new CSV
    with open(out_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "recommendationid",
            "steamid",
            "review",
            "timestamp_created",
            "timestamp_updated",
            "voted_up",
            "weighted_vote_score",
            "playtime_forever",
            "playtime_at_review",
            "last_played",
            "raw_json",
        ])
        writer.writerows(rows)

    print(f"[{db_path}] Wrote {len(rows)} reviews to 90-day CSV: {out_csv_path}")


def main():
    for game in GAMES:
        appid = game["appid"]
        slug = game["slug"]
        title = game["title"]
        release_date = game["release_date"]

        db_path = f"{slug}_reviews.db"
        csv_path_all = f"{slug}_reviews.csv"
        csv_path_90 = f"{slug}_reviews_first90d.csv"

        print(f"\n===== Starting {title} (appid {appid}) =====")
        print(f"DB:           {db_path}")
        print(f"CSV (all):    {csv_path_all}")
        print(f"CSV (90 days): {csv_path_90}")

        conn = init_db(db_path)
        csv_file, csv_writer = init_csv(csv_path_all)

        try:
            fetch_all_reviews(appid, conn, csv_writer)
        finally:
            conn.close()
            csv_file.close()

        # Now create the truncated 90-day CSV from the DB
        export_first_90_days_csv(db_path, csv_path_90, release_date)

        print(f"===== Finished {title} =====\n")


if __name__ == "__main__":
    main()
