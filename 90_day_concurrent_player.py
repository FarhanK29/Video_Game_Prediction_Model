import requests
import json
import sqlite3
import csv
from datetime import datetime, timedelta, timezone



def init_db(db_path: str):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS reviews (
            timestamp INTEGER PRIMARY KEY,
            player_count INTEGER
        )
    """)
    conn.commit()
    return conn


def init_csv(csv_path: str):
    f = open(csv_path, mode="w", newline="", encoding="utf-8")
    writer = csv.writer(f)
    writer.writerow([
        "timestamp",
        "player_count"
    ])
    return f, writer



def save_player_csv(writer, data):
    writer.writerow([
        data[0],
        data[1]
    ])


def save_player_db(conn, data):
    cur = conn.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO reviews (
            timestamp,
            player_count
        ) VALUES (?, ?)
    """, (
        data[0],
        data[1]))
    conn.commit()
    dt = datetime.fromtimestamp(int(data[0])/1000, tz=timezone.utc)
    formatted_date = dt.strftime("%Y-%m-%d")
    print(formatted_date)
    


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
    cur.execute("""SELECT timestamp FROM reviews LIMIT 1""")
    
    release_ts = cur.fetchall()[0][0]
    dt = datetime.fromtimestamp(release_ts)
    formatted_date = dt.strftime("%Y-%m-%d")
    end_ts = int(datetime.strptime(formatted_date, "%Y-%m-%d").timestamp())

    cur.execute("""
        SELECT
            timestamp,
            player_count
        FROM reviews
        WHERE timestamp BETWEEN ? AND ?
        ORDER BY timestamp ASC
    """, (release_ts, end_ts))

    rows = cur.fetchall()
    conn.close()

    # Write filtered rows to a new CSV
    with open(out_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "timestamp",
            "player_count"
        ])
        print(rows)
        writer.writerows(rows)

    print(f"[{db_path}] Wrote {len(rows)} reviews to 90-day CSV: {out_csv_path}")




def get_concurrent_players(conn,appid,writer):
    url = f'https://steamcharts.com/app/{appid}/chart-data.json'
    
    resp = requests.get(url,timeout=5)
    resp.raise_for_status()
    data = resp.json()
    for row in data:
        save_player_db(conn,row)
        save_player_csv(writer,row)
        


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

def main():
    for game in GAMES:
        appid = game["appid"]
        slug = game["slug"]
        title = game["title"]
        release_date = game["release_date"]

        db_path = f"{slug}_players.db"
        csv_path_all = f"{slug}_players.csv"
        csv_path_90 = f"{slug}_players_first90d.csv"

        print(f"\n===== Starting {title} (appid {appid}) =====")
        print(f"DB:           {db_path}")

        conn = init_db(db_path)
        csv_file, csv_writer = init_csv(csv_path_all)

        try:
            get_concurrent_players(conn,appid,csv_writer)
        finally:
            conn.close()

        # export_first_90_days_csv(db_path, csv_path_90, release_date)



if __name__ == '__main__':
    main()