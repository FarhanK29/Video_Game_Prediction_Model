import csv
import os
from pathlib import Path

import nltk
import pandas as pd
from nltk.sentiment.vader import SentimentIntensityAnalyzer

# Make sure VADER lexicon is available
nltk.download("vader_lexicon", quiet=True)

# Folders / files
REVIEWS_DIR = Path("reviews_data")
OUTPUT_CSV = Path("data/combined_reviews_with_sentiment.csv")
GLOB_PATTERN = "*_reviews.csv"

# Playtime buckets (in hours)
def bucket_playtime(hours: float | None) -> str | None:
    if hours is None:
        return None
    try:
        h = float(hours)
    except (TypeError, ValueError):
        return None

    if h < 5:
        return "low"
    elif h < 50:
        return "medium"
    else:
        return "high"


def main():
    sid = SentimentIntensityAnalyzer()

    all_dfs = []

    files = sorted(REVIEWS_DIR.glob(GLOB_PATTERN))
    if not files:
        print(f"No CSV files found in {REVIEWS_DIR} matching pattern {GLOB_PATTERN}")
        return

    print(f"Found {len(files)} files to process:")
    for p in files:
        print("  -", p.name)

    for csv_path in files:
        print(f"\nProcessing {csv_path} ...")

        # Derive game_slug from filename,
        stem = csv_path.stem
        if stem.endswith("_reviews_first90d"):
            game_slug = stem.removesuffix("_reviews_first90d")
        elif stem.endswith("_reviews"):
            game_slug = stem.removesuffix("_reviews")
        else:
            game_slug = stem

        df = pd.read_csv(csv_path)

        # Ensure we have the columns we expect
        if "review" not in df.columns:
            print(f"  [WARN] 'review' column missing in {csv_path}, skipping.")
            continue

        # Compute playtime in hours (Steam gives minutes)
        if "playtime_forever" in df.columns:
            df["playtime_hours"] = df["playtime_forever"] / 60.0
        else:
            df["playtime_hours"] = None

        # VADER sentiment on the review text
        # Fill NaN with empty strings to avoid crashes
        texts = df["review"].astype(str).fillna("")
        df["sentiment_compound"] = texts.apply(lambda t: sid.polarity_scores(t)["compound"]) 

        # Playtime bucket
        df["playtime_bucket"] = df["playtime_hours"].apply(bucket_playtime)

        # Add game identifier
        df["game_slug"] = game_slug

        all_dfs.append(df)

    # Combine everything
    combined = pd.concat(all_dfs, ignore_index=True)

    # Optional: keep only columns you care about
    cols = [
        "game_slug",
        "recommendationid",
        "steamid",
        "review",
        "playtime_forever",
        "playtime_hours",
        "playtime_bucket",
        "timestamp_created",
        "timestamp_updated",
        "voted_up",
        "weighted_vote_score",
        "last_played",
        "sentiment_compound",
        "raw_json",
    ]
    # Filter to existing columns (some might not be present in all files)
    cols = [c for c in cols if c in combined.columns]

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(OUTPUT_CSV, index=False, quoting=csv.QUOTE_MINIMAL)
    print(f"\nWrote combined dataset with sentiment to: {OUTPUT_CSV}")
    print(f"Total rows: {len(combined)}")


if __name__ == "__main__":
    main()
