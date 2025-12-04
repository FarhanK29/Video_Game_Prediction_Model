import requests
import time
import json
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path
import os


GAME_CSV_PATH = "data/games_list.csv"  
FILE_SIZE_LIMIT_BYTES = 40 * 1024 * 1024  # 40 MB per game

#Number of games we're going to sample from the full list to collect reviews for
N_GAMES_SAMPLE = 5

# fixed random seed so the same games are chosen every time
RANDOM_SEED = 1236


def init_csv(csv_path):
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


def save_review_csv(writer, r, raw_json_str):
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
        raw_json_str,
    ])


def fetch_all_reviews_to_csv(app_id, csv_writer, csv_file, max_bytes):
    """
    Fetch Steam reviews for one app_id, writing directly to CSV,
    but stop once the actual CSV file size on disk reaches max_bytes.
    """
    steam_reviews_url = f"https://store.steampowered.com/appreviews/{app_id}"
    cursor = "*"
    total = 0
    unique = set()

    # include header row in size
    csv_file.flush()
    bytes_used = os.path.getsize(csv_file.name)

    while True:
        if bytes_used >= max_bytes:
            print(f"[appid {app_id}] Reached size limit ~{max_bytes / (1024*1024):.1f} MB, stopping.")
            break

        params = {
            "json": 1,
            "language": "english",
            "filter": "recent",
            "review_type": "all",
            "purchase_type": "all",
            "num_per_page": 100,
            "cursor": cursor,
        }

        resp = requests.get(steam_reviews_url, params=params, timeout=3)

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
            if rec_id in unique:
                continue

            raw_json_str = json.dumps(r, ensure_ascii=False)

            # write the row
            save_review_csv(csv_writer, r, raw_json_str)
            total += 1
            unique.add(rec_id)

            # flush and check actual file size
            csv_file.flush()
            bytes_used = os.path.getsize(csv_file.name)

            if bytes_used >= max_bytes:
                print(
                    f"[appid {app_id}] Hit file size limit after writing row: "
                    f"{bytes_used / (1024*1024):.2f} MB"
                )
                break

        if bytes_used >= max_bytes:
            break

        new_cursor = data.get("cursor")
        if not new_cursor or new_cursor == cursor:
            print(f"[appid {app_id}] cursor did not advance, stopping.")
            break

        cursor = new_cursor
        print(f"[appid {app_id}] Fetched {total} reviews so far, file size ~{bytes_used / (1024*1024):.2f} MB")

    print(
        f"[appid {app_id}] Done. Total unique reviews saved: {len(unique)}, "
        f"final file size: {bytes_used / (1024*1024):.2f} MB"
    )



def export_first_90_days_csv(all_csv_path, out_csv_path, release_date_str):
    """
    Read from the per-game 'all reviews' CSV and write a CSV containing
    only reviews within 90 days of release_date_str (YYYY-MM-DD).
    If release_date_str is None/empty, skip.
    """
    if not release_date_str:
        print(f"[{all_csv_path}] No release_date provided, skipping 90-day export.")
        return

    release_dt = datetime.strptime(release_date_str, "%Y-%m-%d")
    end_dt = release_dt + timedelta(days=90)
    release_ts = int(release_dt.timestamp())
    end_ts = int(end_dt.timestamp())

    print(f"[{all_csv_path}] 90-day window: {release_dt} -> {end_dt}")

    with open(all_csv_path, newline="", encoding="utf-8") as f_in, \
         open(out_csv_path, "w", newline="", encoding="utf-8") as f_out:

        reader = csv.DictReader(f_in)

        fieldnames = [
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
        ]
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()

        rows_written = 0
        for row in reader:
            ts_str = row.get("timestamp_created")
            if not ts_str:
                continue
            try:
                ts = int(ts_str)
            except ValueError:
                continue

            if release_ts <= ts <= end_ts:
                writer.writerow(row)
                rows_written += 1

    print(f"[{all_csv_path}] Wrote {rows_written} reviews to 90-day CSV: {out_csv_path}")


def load_games_from_csv(path):
    games = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Expecting columns: name, slug, appid, release_date
            try:
                appid = int(row["appid"])
            except (ValueError, KeyError):
                continue

            games.append({
                "title": row.get("name", "").strip() or f"appid_{appid}",
                "slug": (row.get("slug") or f"appid_{appid}").strip(),
                "appid": appid,
                "release_date": (row.get("release_date") or "").strip() or None,
            })
    return games


def main():
    games = load_games_from_csv(GAME_CSV_PATH)
    print(f"Loaded {len(games)} games from {GAME_CSV_PATH}")

    random.seed(RANDOM_SEED)
    random.shuffle(games)
    games_sample = games[:N_GAMES_SAMPLE]  # take first N after shuffling

    print(f"Sampling {len(games_sample)} games (seed={RANDOM_SEED}):")
    for g in games_sample:
        print(f"  - {g['title']} (appid {g['appid']})")

    Path("reviews_data").mkdir(exist_ok=True)

    for game in games_sample:
        appid = game["appid"]
        slug = game["slug"]
        title = game["title"]
        release_date = game["release_date"]

        csv_path_all = f"reviews_data/{slug}_reviews.csv"
        csv_path_90 = f"reviews_data/{slug}_reviews_first90d.csv"

        print(f"\n===== Starting {title} (appid {appid}) =====")
        print(f"CSV (all):     {csv_path_all}")
        print(f"CSV (90 days): {csv_path_90}")

        csv_file, csv_writer = init_csv(csv_path_all)
        try:
            fetch_all_reviews_to_csv(appid, csv_writer, csv_file, FILE_SIZE_LIMIT_BYTES)
        finally:
            csv_file.close()

        # Now create the truncated 90-day CSV from the "all reviews" CSV
        export_first_90_days_csv(csv_path_all, csv_path_90, release_date)

        print(f"===== Finished {title} =====\n")


if __name__ == "__main__":
    main()
