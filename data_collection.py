import csv
import time
import re
from datetime import datetime

import requests


#CONFIG
BASE_URL = "https://api.gamalytic.com/game/{}"
OUTPUT_CSV = "data/games_list.csv"


SEED_GAMES = [
    {"title": "Grand Theft Auto V", "appid": 271590},
    {"title": "Grand Theft Auto IV: The Complete Edition", "appid": 12210},
    {"title": "Red Dead Redemption 2", "appid": 1174180},
    {"title": "Watch_Dogs", "appid": 243470},
    {"title": "Watch Dogs: Legion", "appid": 2239550},
    {"title": "Sleeping Dogs: Definitive Edition", "appid": 307690},
    {"title": "Saints Row IV: Re-Elected", "appid": 206420},
    {"title": "Mafia III: Definitive Edition", "appid": 360430},
    {"title": "Cyberpunk 2077", "appid": 1091500},
    {"title": "Yakuza 0", "appid": 638970},
]


#Converts full name to slug for our CSV output for each game later
def get_slug_name(name):
    s = name.lower()
    s = re.sub(r"[^0-9a-z]+", "_", s)
    s = s.strip("_")
    return s


def ms_to_date(ms):
    if not ms:
        return None
    # Gamalytic timestamps are in milliseconds, so divide by 1000.
    dt = datetime.utcfromtimestamp(ms / 1000.0)
    return dt.date().isoformat()


def add_or_update_game(store: dict, steam_id: str | int, name: str, release_ms) -> None:
    """
    Add a game to the dictionary (or update its info if we already saw it).
    Keyed by appid (int).
    """
    try:
        appid = int(steam_id)
    except (ValueError, TypeError):
        # Skip weird / missing IDs
        return

    #Do conversions
    release_date = ms_to_date(release_ms)
    slug = get_slug_name(name)

    # add or update each entry 
    if appid not in store:
        store[appid] = {
            "name": name,
            "slug": slug,
            "appid": appid,
            "release_date": release_date,
        }
    else:
        #If it's seed data we just update the date since we didn't include that
        if store[appid].get("release_date") is None and release_date is not None:
            store[appid]["release_date"] = release_date

def fetch_game_data(appid: int) -> dict | None:
    """
    Call the Gamalytic /game/{appid} endpoint and return JSON.
    Returns None on error.
    """
    url = BASE_URL.format(appid)
    try:
        resp = requests.get(url, timeout=10)
    except requests.RequestException as e:
        print(f"[ERROR] Request failed for appid {appid}: {e}")
        return None

    if resp.status_code != 200:
        print(f"[ERROR] HTTP {resp.status_code} for appid {appid}: {resp.text[:200]}")
        return None

    try:
        return resp.json()
    except ValueError as e:
        print(f"[ERROR] JSON parse error for appid {appid}: {e}")
        return None


def main():
    # Store all games in this dictionary
    games: dict[int, dict] = {}

    # Add our seed games to the dict
    for seed in SEED_GAMES:
        appid = seed["appid"]
        games[appid] = {
            "name": seed["title"],
            "slug": get_slug_name(seed["title"]),
            "appid": appid,
            "release_date": None,  # to be filled from Gamalytic if available
        }

    # Fetch the audienceOverlap games for each seed game to build our full list of games
    for seed in SEED_GAMES:
        appid = seed["appid"]
        print(f"Fetching Gamalytic data for seed appid {appid} ({seed['title']})...")
        data = fetch_game_data(appid)
        if not data:
            continue

        #Add our new game or update our seed game info
        add_or_update_game(
            games,
            data.get("steamId"),
            data.get("name", seed["title"]),
            data.get("firstReleaseDate") or data.get("releaseDate"),
        )

        # Add all audienceOverlap games
        for entry in data.get("audienceOverlap", []):
            add_or_update_game(
                games,
                entry.get("steamId"),
                entry.get("name", ""),
                entry.get("releaseDate"),
            )

        

        #I think they do rate limiting
        time.sleep(1)



    #Write to CSV

    fieldnames = ["name", "slug", "appid", "release_date"]

    print(f"\nCollected {len(games)} unique games. Writing to {OUTPUT_CSV} ...")

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        # Sort by appid
        for appid in sorted(games.keys()):
            writer.writerow(games[appid])

    print("Done.")


if __name__ == "__main__":
    main()
