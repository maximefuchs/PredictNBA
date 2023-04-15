from nba_api.stats.endpoints import LeagueGameFinder, BoxScoreTraditionalV2
import pandas as pd
import datetime
import time

# get a dataset for each year
for year in range(2023-nb_years,2023):
  # Create an empty list to store game data
  game_data = []
  # get all games from a specific year
  games = game_stats.loc[game_stats['GAME_DATE'] >= str(year) + "-10-01"].reset_index(drop=True)
  games = games.loc[games['GAME_DATE'] < str(year+1) + "-08-01"].reset_index(drop=True)

  # Loop through each game and retrieve the players who played
  for index, row in games.iterrows():
      game_id = row['GAME_ID']
      home_team_id = row['HOME_TEAM_ID']
      away_team_id = row['AWAY_TEAM_ID']
      outcome = row['HOME_TEAM_WIN']

      print(year,index,game_id)
      
      # Use BoxScoreTraditionalV2 endpoint to retrieve player data for each game
      while(True):
        try:
          boxscore = BoxScoreTraditionalV2(game_id=game_id,timeout=40)
          time.sleep(0.2)
          break
        except Exception as e:
          print(f"Error: {e} | Retry")

      player_stats = boxscore.get_data_frames()[0]
      
      # Filter out non-player rows
      player_stats = player_stats.loc[player_stats['PLAYER_ID'].notnull()]
      
      # Add the player data to the game_data list
      game_data.append(player_stats)
      
  # Concatenate all game_data into a single dataframe
  player_data = pd.concat(game_data, axis=0)

  # Print the first 5 rows of the player_data dataframe
  print(player_data)
  player_data.to_csv(f"players_data_{year}.csv")
