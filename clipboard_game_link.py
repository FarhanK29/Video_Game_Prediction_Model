import webbrowser
import pandas as pd
import time
import pyautogui


GAME_LIST_CSV = "data/games_list.csv"
BASE_URL = "https://steamdb.info/app/{}/charts/#max"

# df = pd.read_csv(GAME_LIST_CSV)

# # iterates through each of the games and goes to steamdb so you can manually download csv
# for i in range (len(df)):
#     appid = df.iloc[i]['appid']
#     url = BASE_URL.format(appid)
#     webbrowser.open(url)
#     time.sleep(10) # to avoid rate limiting
#     pyautogui.hotkey('ctrl', 'w') # to close tab


# iterate through to update these concurrent players as we were rate limited 
data = [220, 320, 360, 550, 7670, 8850, 9480, 729040, 1030840, 1086940, 1088710, 1091500, 1105500, 1105510, 1113000, 1172470, 1174180, 1235140, 1237970, 1238810, 1245620, 1388590, 2096600, 2096610, 2208920, 2221920, 2231380, 2239550, 2369390]
i = 0
for appid in data:
    print (f'{i}/{len(data)}')
    url = BASE_URL.format(appid)
    webbrowser.open(url)
    time.sleep(15) # to avoid rate limiting
    pyautogui.hotkey('ctrl', 'w') # to close tab


