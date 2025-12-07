import requests
from datetime import datetime, timedelta
import pandas as pd
import webbrowser
from pathlib import Path
import time
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from avg_sentiment import fetch_filtered_reviews_sentiment
import numpy as np
import re


# Gets appid for steam game using steam api
def fetch_game(game_name:str):
    steam_url = "https://store.steampowered.com/api/storesearch/"
    params = {
        "term": game_name,
        "l": "english",
        "cc": "US"
    }

    try:
        resp = requests.get(steam_url,params=params)
        
        if resp.status_code != 200 :
            print('Error finding game')
            return None
        
        data = resp.json()

        if data['total'] > 0:
            search_len = min(data['total'],6)
            for i in range(1,search_len):
                print(f'{i}. {data['items'][i-1]['name']}')
            game_choice = int(input('Type the number of the game you want: '))
            if game_choice > search_len or game_choice < 1:
                print('Error: Incorrect range of values')
                return None
            game = data['items'][game_choice-1]
            return game['id']
        else:
            print("No Games found")
            return None
    except Exception as e:
        print(f'Error finding game: {e}')
        return None

# returns dictionary of retrieved gamalytic data
def fetch_gamalytic_data(appid):

    gamalytic_url = "https://api.gamalytic.com/game/{}"

    response = requests.get(gamalytic_url.format(appid))

    if response.status_code != 200:
        print("Error! Unable to fetch from URL")
        return None
    
    data = response.json()
    launch_data = data['history'][0]
    # getting all the data from the json 
    name = data['name'].encode("ascii", errors="ignore").decode()
    slug = re.sub(r'[^A-Za-z0-9 ]+', '', name.lower()).replace(" ", "_")
    appid = appid 
    timestamp_s = int(data['releaseDate']) / 1000
    release_date = datetime.fromtimestamp(timestamp_s).strftime("%Y-%m-%d")
    total_reviews = int(data['reviews'])
    estimated_launch_reviews = int(launch_data['reviews'])  if int(launch_data['reviews'] or 0) > 0  else int(data['reviews']) * 0.1
    followers =   int(launch_data['followers']) if int(launch_data['followers'] or 0) > 0 else int(data['followers']) * 0.1
    review_score = int(data['reviewScore'])
    avg_playtime = int(launch_data['avgPlaytime']) if int(launch_data['avgPlaytime'] or 0) > 0 else int(data['avgPlaytime'])
    copies_sold = int(data['copiesSold'])
    revenue = int(data['revenue'])
    players = int(launch_data['players']) if int(launch_data['players']) > 0 else int(data['players'])
    owners = data['owners']
    estimated_launch_followers = int(launch_data['followers']) if int(launch_data['followers']) > 0 else int(data['followers']) * 0.1
    estimated_launch_copies_sold =  int(data['copiesSold']) * 0.1
    developers = data['developers']
    publishers = data['publishers']

    if (developers == []):
            developers = publishers
    if (publishers == []):
        publishers = developers
    developer = developers[0]
    publisher = publishers[0]

    new_game = {
        'name' : name,
        'slug' : slug,
        'appid' : appid,
        'release_date' : release_date,
        'total_reviews': total_reviews,
        'estimated_launch_reviews' : estimated_launch_reviews,
        'followers' : followers,
        'review_score' : review_score,
        'avg_playtime' : avg_playtime,
        'copies_sold' : copies_sold,
        'revenue' : revenue,
        'players' : players,
        'owners' : owners,
        'estimated_launch_copies_sold' : estimated_launch_copies_sold,
        'estimated_launch_followers' : estimated_launch_followers,
        'developer' : developer,
        'publisher' : publisher 
    }
    return new_game
# mssing avg_sentiment, peak_concurrent_players_after_90, peak_concurrent_players_timestamp,avg_concurrent_players_after_90

# makes user download csv from steamdb and retrieves concurrent user data from it 
def get_concurrent_player_data(game_dict):
    appid = game_dict['appid']

    steam_db_url ="https://steamdb.info/app/{}/charts/#max"
    url = steam_db_url.format(appid)
    webbrowser.open(url)
    file_path = Path("game_concurrent_players/steamdb_chart_{}.csv".format(appid))

    print("Waiting for concurrent player csv ...")
    while not file_path.exists():
        time.sleep(2)


    release_date = pd.to_datetime(game_dict['release_date'])
    release_date_90 = release_date + timedelta(days=90)

    chart_df = pd.read_csv(file_path)
    chart_df['DateTime'] = pd.to_datetime(chart_df['DateTime'])

    if (chart_df.loc[0,'DateTime'] > release_date):
        release_date = chart_df.loc[0,'DateTime']
        release_date_90 = release_date + timedelta(days=90)
        print(f"{appid}: Data for {game_dict['release_date']} unavailable. Setting new release date to {release_date}")
        return None
    

    chart_df = chart_df.dropna(thresh=2)
    chart_df = chart_df.drop(['Average Players'],axis=1)

    chart_df = chart_df[(chart_df['DateTime'] >= release_date) & (chart_df['DateTime'] <= release_date_90)]
    
    peak_row = chart_df.loc[chart_df['Players'].idxmax()]
    peak_players = peak_row['Players']
    peak_timestamp = peak_row['DateTime']
    avg_players = chart_df.loc[:, chart_df.columns == 'Players'].mean()


    game_dict['peak_concurrent_players_after_90'] = peak_players.item()
    game_dict['peak_concurrent_players_timestamp'] = peak_timestamp
    game_dict['avg_concurrent_players_after_90'] = avg_players.item()

    return game_dict

# gets the sentiment from reviews
def get_sentiment_data(game_dict):
    sid = SentimentIntensityAnalyzer()
    print("Calculating average sentiment...")
    avg_sent, count = fetch_filtered_reviews_sentiment(game_dict['appid'],sid,1000)  
    if avg_sent is None:
        print(f"No reviews found for this game")
        avg_sent = np.nan
    game_dict['avg_sentiment'] = avg_sent
    return game_dict



def main():
    # run this the first time using nltk 
    # import nltk
    # nltk.download('vader_lexicon')

    # Set these to the same file if you want to append to the csv
    INPUT_GAME_LIST_CSV = "data/updated_game_info.csv"
    OUTPUT_GAME_LIST_CSV = "data/updated_game_info.csv"
    
    user_input = str(input('Enter name of game or type exit to quit: '))

    while user_input.lower() != 'exit':
        appid = fetch_game(user_input)
        new_game = fetch_gamalytic_data(appid=appid)
        # print(f'Gamalytic info: {new_game}')
        print('appid retrieved!')
        new_game_concurrent = get_concurrent_player_data(new_game)
        # print(f'Concurrent player info: {new_game_concurrent}')
        print(f'Average concurrent players for {new_game['name']} is {new_game['avg_concurrent_players_after_90']}')
        new_game_sentiment = get_sentiment_data(new_game_concurrent)
        print(f'Average sentiment of {new_game['name']}: {new_game_sentiment['avg_sentiment']}')
        game_list = pd.read_csv(INPUT_GAME_LIST_CSV)
        game_list = game_list._append(new_game_sentiment, ignore_index=True)
        game_list.to_csv(OUTPUT_GAME_LIST_CSV,index=False)
        user_input = str(input('Enter name of game or type exit to quit: '))

if __name__ == '__main__':
    main()