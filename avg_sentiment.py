import csv
import time
import json
import requests
from pathlib import Path

import pandas as pd
from nltk.sentiment.vader import SentimentIntensityAnalyzer


# Paths
GAMES_LIST_PATH = Path("data/games_list.csv")         # has at least: name, slug, appid, release_date
GAMES_DATA_PATH = Path("data/games_data_list.csv")    # full feature table you described
OUTPUT_PATH     = Path("data/games_data_list_with_sentiment.csv")

# Review filters
MIN_PLAYTIME_HOURS = 5          # >= 5 hours
MIN_WORDS_PER_REVIEW = 5        # >= 5 words
TARGET_REVIEWS_PER_GAME = 1000  # try to collect this many matching reviews per game

# Steam reviews endpoint
STEAM_REVIEWS_URL = "https://store.steampowered.com/appreviews/{appid}"


def load_games_from_csv(path: Path):
    """Load games from the minimal games_list.csv (for appids & slugs)."""
    games = []
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                appid = int(row["appid"])
            except (ValueError, KeyError):
                continue
            games.append({
                "name": (row.get("name") or "").strip(),
                "slug": (row.get("slug") or f"appid_{appid}").strip(),
                "appid": appid,
            })
    return games


def fetch_filtered_reviews_sentiment(appid: int,
                                     sid: SentimentIntensityAnalyzer,
                                     target_n: int = TARGET_REVIEWS_PER_GAME):
    """
    Fetch reviews from Steam for a given appid and compute VADER compound
    sentiment for up to `target_n` reviews that match:
      - language = English (handled by API)
      - playtime >= MIN_PLAYTIME_HOURS
      - review has >= MIN_WORDS_PER_REVIEW words
    Returns (avg_sentiment, count_used).
    """
    url = STEAM_REVIEWS_URL.format(appid=appid)
    cursor = "*"
    collected_scores = []
    min_playtime_minutes = MIN_PLAYTIME_HOURS * 60

    while len(collected_scores) < target_n:
        params = {
            "json": 1,
            "language": "english",
            "filter": "recent",      # could also use 'all'; 'recent' is fine
            "review_type": "all",
            "purchase_type": "all",
            "num_per_page": 100,
            "cursor": cursor,
        }

        resp = requests.get(url, params=params, timeout=3)

        if resp.status_code == 429:
            print(f"[appid {appid}] rate limited, sleeping 10s...")
            time.sleep(10)
            continue

        resp.raise_for_status()
        data = resp.json()

        if data.get("success") != 1:
            print(f"[appid {appid}] request unsuccessful, stopping.")
            break

        reviews = data.get("reviews", [])
        if not reviews:
            print(f"[appid {appid}] no more reviews from API, stopping.")
            break

        for r in reviews:
            # Text filter
            text = (r.get("review") or "").strip()
            if not text:
                continue
            if len(text.split()) < MIN_WORDS_PER_REVIEW:
                continue

            # Playtime filter
            author = r.get("author", {}) or {}
            playtime_forever = author.get("playtime_forever")
            if playtime_forever is None:
                continue
            try:
                playtime_forever = int(playtime_forever)
            except (TypeError, ValueError):
                continue

            if playtime_forever < min_playtime_minutes:
                continue

            # Passed filters → compute VADER sentiment
            compound = sid.polarity_scores(text)["compound"]
            collected_scores.append(compound)

            if len(collected_scores) >= target_n:
                break

        new_cursor = data.get("cursor")
        if not new_cursor or new_cursor == cursor:
            # No further pages
            break
        cursor = new_cursor

    if not collected_scores:
        return None, 0

    avg_sentiment = sum(collected_scores) / len(collected_scores)
    return avg_sentiment, len(collected_scores)


def main():
    # Load the list of games for which we want sentiment
    games = load_games_from_csv(GAMES_LIST_PATH)
    print(f"Loaded {len(games)} games from {GAMES_LIST_PATH}")

    sid = SentimentIntensityAnalyzer()

    results = []

    for i, game in enumerate(games, start=1):
        appid = game["appid"]
        name = game["name"] or f"appid_{appid}"
        print(f"\n[{i}/{len(games)}] Processing {name} (appid {appid})")

        avg_sent, count = fetch_filtered_reviews_sentiment(appid, sid)

        if avg_sent is None:
            print(f"  → No qualifying reviews found for this game.")
        else:
            print(f"  → Used {count} reviews, avg sentiment = {avg_sent:.4f}")

        results.append({
            "appid": appid,
            "avg_sentiment_vader": avg_sent,
            "n_reviews_sentiment": count,
        })

    # Turn results into a DataFrame
    sentiment_df = pd.DataFrame(results)

    # Load the full games_data_list.csv
    games_data_df = pd.read_csv(GAMES_DATA_PATH)

    # Make sure appid types align for merge
    games_data_df["appid"] = pd.to_numeric(games_data_df["appid"], errors="coerce").astype("Int64")
    sentiment_df["appid"] = pd.to_numeric(sentiment_df["appid"], errors="coerce").astype("Int64")

    # Merge on appid
    merged = games_data_df.merge(sentiment_df, on="appid", how="left")

    # Save to new file
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUTPUT_PATH, index=False)

    print(f"\nWrote merged games data with sentiment to: {OUTPUT_PATH}")
    print(f"Shape: {merged.shape}")
    print("Columns now include: avg_sentiment_vader and n_reviews_sentiment")


if __name__ == "__main__":
    main()
