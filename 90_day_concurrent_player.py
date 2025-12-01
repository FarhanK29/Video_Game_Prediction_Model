import requests
import json
import sqlite3
import csv



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
        data[1]    ))
    conn.commit()





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
    print ('hi')
    for game in GAMES:
        appid = game["appid"]
        slug = game["slug"]
        title = game["title"]
        release_date = game["release_date"]

        db_path = f"{slug}_players.db"
        csv_path_all = f"{slug}_players.csv"

        print(f"\n===== Starting {title} (appid {appid}) =====")
        print(f"DB:           {db_path}")

        conn = init_db(db_path)
        csv_file, csv_writer = init_csv(csv_path_all)

        try:
            get_concurrent_players(conn,appid,csv_writer)
        finally:
            conn.close()



if __name__ == '__main__':
    main()