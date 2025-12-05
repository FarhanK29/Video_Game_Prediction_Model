import pandas as pd
import numpy as np
from datetime import timedelta

GAME_LIST_CSV = "data/games_data_list.csv"
OUTPUT_DIR = "cleaned_concurrent_players"
INPUT_CSV = "game_concurrent_players/steamdb_chart_{}.csv"



df = pd.read_csv(GAME_LIST_CSV)
data = []

for i in range(len(df)):

    appid = df.iloc[i]['appid']
    rel_date = df.iloc[i]['release_date']
    release_date = pd.to_datetime(rel_date)
    release_date_90 = release_date + timedelta(days=90)
    
    chart_df = pd.read_csv(INPUT_CSV.format(appid))
    chart_df['DateTime'] = pd.to_datetime(chart_df['DateTime'])

    if (chart_df.loc[0,'DateTime'] > release_date):
        release_date = chart_df.loc[0,'DateTime']
        release_date_90 = release_date + timedelta(days=90)
        print(f"{appid}: Data for {rel_date} unavailable. Setting new release date to {release_date}")
        data.append(int(appid))

    chart_df = chart_df.dropna(thresh=2)
    chart_df = chart_df.drop(['Average Players'],axis=1)

    chart_df = chart_df[(chart_df['DateTime'] >= release_date) & (chart_df['DateTime'] <= release_date_90)]
    
    peak_row = chart_df.loc[chart_df['Players'].idxmax()]
    peak_players = peak_row['Players']
    peak_timestamp = peak_row['DateTime']
    avg_players = chart_df.loc[:, chart_df.columns == 'Players'].mean()


    df.loc[i,'peak_concurrent_players_after_90'] = peak_players.item()
    df.loc[i,'peak_concurrent_players_timestamp'] = peak_timestamp
    df.loc[i,'avg_concurrent_players_after_90'] = avg_players.item()

    chart_df.to_csv(f'{OUTPUT_DIR}/{appid}.csv',index=False)

df.to_csv(GAME_LIST_CSV,index=False)

print(data)


