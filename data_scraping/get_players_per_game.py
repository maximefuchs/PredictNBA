from nba_api.stats.endpoints import BoxScoreTraditionalV2
import pandas as pd
import time

nb_years = 10
game_stats = pd.read_csv(f"data/games{nb_years}years.csv")

start_year = 2023-nb_years
# get a dataset for each year
for year in range(start_year,2023):
  # Create an empty list to store game data
  game_data = []
  # get all games from a specific year
  games = game_stats.loc[game_stats['GAME_DATE'] >= str(year) + "-10-01"].reset_index(drop=True)
  games = games.loc[games['GAME_DATE'] < str(year+1) + "-08-01"].reset_index(drop=True)

  # Loop through each game and retrieve the players who played
  for index, row in games.iterrows():
    game_id = str(row['GAME_ID'])
    game_date = row['GAME_DATE']
    home_team_id = row['HOME_TEAM_ID']
    away_team_id = row['AWAY_TEAM_ID']
    outcome = row['HOME_TEAM_WIN']

    # for syntax purposes
    while len(game_id) < 10:
      game_id = "0" + game_id
    print(index,game_date,game_id,row['GAME_DATE'])
    
    # boxscore = BoxScoreTraditionalV2(game_id=str(game_id),timeout=40)
    while(True):
      try:
        boxscore = BoxScoreTraditionalV2(game_id=game_id,timeout=40)
        time.sleep(0.1)
        break
      except Exception as e:
        print(f"{e} | Retry")

    player_stats = boxscore.get_data_frames()[0]
    
    # Filter out non-player rows
    player_stats = player_stats.loc[player_stats['PLAYER_ID'].notnull()]
    
    # Add the player data to the game_data list
    game_data.append(player_stats)
    
  # Concatenate all game_data into a single dataframe
  player_data = pd.concat(game_data, axis=0)

  # Print the first 5 rows of the player_data dataframe
  print(player_data)
  player_data.to_csv(f"data/players_data_{year}.csv")
