import csv
import requests
import pandas as pd
import numpy as np



BASE_URL = "https://api.gamalytic.com/game/{}"
GAME_LIST_CSV = "data/games_data_list.csv"
OUTPUT_CSV = "data/games_data_list.csv"

def get_gamalytic_info(appid:str):
    response = requests.get(BASE_URL.format(appid))

    if response.status_code != 200:
        print("Error! Unable to fetch from URL")
        return None
    
    data = response.json()
    return data
    

def add_game_data():

    df = pd.read_csv(GAME_LIST_CSV)

    for i in range(len(df)):
        # cleans game name to remove the trademark symbols
        df.loc[i,'name'] = df.iloc[i]['name'].encode("ascii", errors="ignore").decode()
        print(f"{i+1}/{len(df)}: {df.iloc[i]['name'].encode("ascii", errors="ignore").decode()}")

        appid = df.iloc[i]['appid']

        data = get_gamalytic_info(appid)

        df.loc[i,'total_reviews'] = data.get('reviewsSteam') or np.nan
        df.loc[i,'estimated_launch_reviews'] = data.get('reviewsSteam',0) * 0.1
        df.loc[i,'followers'] =  data.get('followers') or np.nan
        df.loc[i,'estimated_launch_followers'] = data.get('followers',0) * 0.1
        df.loc[i,'review_score'] = data.get('reviewScore') or np.nan
        df.loc[i,'avg_playtime'] = data.get('avgPlaytime') or np.nan
        df.loc[i,'copies_sold'] = data.get('copiesSold') or np.nan
        df.loc[i,'estimated_launch_copies_sold'] = data.get('copiesSold',0) * 0.1
        df.loc[i,'revenue'] = data.get('revenue') or np.nan
        df.loc[i,'players'] = data.get('players') or np.nan
        df.loc[i,'owners'] = data.get('owners') or np.nan
    

    df.to_csv(OUTPUT_CSV,index=False)

if __name__ == '__main__':
    add_game_data()



    

    





