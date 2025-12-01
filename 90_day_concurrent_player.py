import requests
import json
import sqlite3



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


def save_review_db(conn, data):
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





def get_concurrent_players(conn,appid):
    url = f'https://steamcharts.com/app/{appid}/chart-data.json'
    
    resp = requests.get(url,timeout=5)
    resp.raise_for_status()
    data = resp.json()
    for row in data:
        save_review_db(conn,row)


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
        csv_path_90 = f"{slug}_players_first90d.csv"

        print(f"\n===== Starting {title} (appid {appid}) =====")
        print(f"DB:           {db_path}")
        print(f"CSV (90 days): {csv_path_90}")

        conn = init_db(db_path)

        try:
            get_concurrent_players(conn,appid)
        finally:
            conn.close()



if __name__ == '__main__':
    main()