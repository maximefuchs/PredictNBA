from nba_api.stats.endpoints import commonallplayers,playercareerstats,commonplayerinfo
import pandas as pd
import datetime
import time
import os

nb_years = 10
current_year = datetime.datetime.now().year

# Make API call and get player data
player_data = commonallplayers.CommonAllPlayers(league_id = '00')
data = player_data.get_data_frames()[0]

data = data.loc[data['TO_YEAR'] > str(current_year-nb_years)] 

ids = data['PERSON_ID'].tolist()
print("number of players:",len(ids))

# Get career stats for each player
career_stats = []
for index,player_id in enumerate(ids):
  if index % 5 == 0: print(f"{index} players retrieved")
  while(True):
    try:
      player_data = playercareerstats.PlayerCareerStats(player_id = str(player_id)).get_data_frames()[0]
      time.sleep(0.1)
      break
    except Exception as e:
      print(f"{e} | Retry on PlayerCareerStats")
  while(True):
    try:
      player_info = commonplayerinfo.CommonPlayerInfo(player_id=player_id).get_data_frames()[0]
      time.sleep(0.1)
      break
    except Exception as e:
      print(f"{e} | Retry on CommonPlayerInfo")
  concat_data = pd.merge(player_data,player_info)
  career_stats.append(concat_data)

# Concatenate data into a single DataFrame
career_stats_df = pd.concat(career_stats, ignore_index=True)

# save as csv
if not os.path.isdir("data"):
    os.mkdir("data")
career_stats_df.to_csv(f"data/players_specs_{nb_years}years.csv")