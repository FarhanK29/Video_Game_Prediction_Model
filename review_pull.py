import requests
import sqlite3
import time
import json
import csv

APP_ID = 2239550  # Watch Dogs: Legion
DB_PATH = "watch_dogs_legion_reviews.db"
CSV_PATH = "watch_dogs_legion_reviews.csv"

STEAM_REVIEWS_URL = f"https://store.steampowered.com/appreviews/{APP_ID}"

def init_db():
    conn = sqlite3.connect(DB_PATH)
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

def init_csv():
    f = open(CSV_PATH, mode="w", newline="", encoding="utf-8")
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

def fetch_all_reviews(conn, csv_writer):
    cursor = "*"
    total = 0
    unique = set()

    while True:
        params = {
            "json": 1,
            "language": "all", #We can switch this to english if we perform any kind of sentiment analysis
            "filter": "recent",
            "review_type": "all",
            "purchase_type": "all",
            "num_per_page": 100,
            "cursor": cursor,
        }

        resp = requests.get(STEAM_REVIEWS_URL, params=params, timeout=30)

        if resp.status_code == 429:
            print("rate limited")
            time.sleep(10)
            continue

        resp.raise_for_status()
        data = resp.json()

        if data.get("success") != 1:
            print("request unsuccessful")
            break

        reviews = data.get("reviews", [])
        if not reviews:
            print("Finished fetching all reviews.")
            break

        for r in reviews:
            rec_id = r.get("recommendationid")
            if rec_id not in unique:
                unique.add(rec_id)
                # Save to DB
                save_review_db(conn, r)
                # Save to CSV
                save_review_csv(csv_writer, r)
                total += 1

        print(f"Fetched {len(reviews)} reviews, (total so far: {total}, unique IDs: {len(unique)})")

        new_cursor = data.get("cursor")
        if not new_cursor or new_cursor == cursor:
            print("cursor did not advance")
            break

        cursor = new_cursor

    print(f"Finished fetching all reviews. Total unique reviews saved: {len(unique)}")



conn = init_db()
csv_file, csv_writer = init_csv()
try:
    fetch_all_reviews(conn, csv_writer)
finally:
    conn.close()
    csv_file.close()


