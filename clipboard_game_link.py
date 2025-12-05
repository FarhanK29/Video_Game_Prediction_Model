import webbrowser
import pandas as pd
import time
import pyautogui


GAME_LIST_CSV = "data/games_list.csv"
BASE_URL = "https://steamdb.info/app/{}/charts"

df = pd.read_csv(GAME_LIST_CSV)

# iterates through each of the games and goes to steamdb so you can manually download csv
for i in range (len(df)):
    appid = df.iloc[i]['appid']
    url = BASE_URL.format(appid)
    webbrowser.open(url)
    time.sleep(10) # to avoid rate limiting
    pyautogui.hotkey('ctrl', 'w') # to close tab




