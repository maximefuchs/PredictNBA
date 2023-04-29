from nba_api.stats.endpoints import LeagueGameFinder
import pandas as pd
import datetime
import os

nb_years = 10
playoffs = True

filename = "Games" + ("Playoffs" if playoffs else "RegularSeason") + "_" + str(nb_years) + "years.csv"

# Set start and end date to retrieve games
end_date = datetime.date.today()
start_date = end_date - datetime.timedelta(days=365 * nb_years)

# Set season type to Regular Season or Playoffs
if playoffs:
    season_type = 'Playoffs'
else:
    season_type = 'Regular Season'

# Use LeagueGameFinder endpoint to search for games
game_finder = LeagueGameFinder(date_from_nullable=start_date.strftime('%m/%d/%Y'),
                               date_to_nullable=end_date.strftime('%m/%d/%Y'),
                               season_type_nullable=season_type,
                               league_id_nullable='00') # 00 for NBA, otherwise we get G-league as well
games = game_finder.get_data_frames()[0]

# Get home and away team statistics for each game
home_stats = games[games['MATCHUP'].str.contains("vs")].copy()
away_stats = games[games['MATCHUP'].str.contains("@")].copy()
# get columns we care about
home_stats = home_stats[['GAME_ID', 'GAME_DATE', 'WL','TEAM_ABBREVIATION', 'TEAM_NAME', 'FGM', 'FGA', 'FG_PCT', 'FTM', 'FTA', 'FT_PCT', 'FG3M', 'FG3A', 'FG3_PCT', 'OREB', 'DREB', 'REB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS']]
away_stats = away_stats[['GAME_ID', 'TEAM_ABBREVIATION', 'TEAM_NAME', 'FGM', 'FGA', 'FG_PCT', 'FTM', 'FTA', 'FT_PCT', 'FG3M', 'FG3A', 'FG3_PCT', 'OREB', 'DREB', 'REB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS']]

# Rename columns to indicate home/away team
home_stats.columns = ['GAME_ID', 'GAME_DATE', 'HOME_TEAM_WIN', 'HOME_TEAM_ID', 'HOME_TEAM_NAME', 'HOME_FGM', 'HOME_FGA', 'HOME_FG_PCT', 'HOME_FTM', 'HOME_FTA', 'HOME_FT_PCT', 'HOME_FG3M', 'HOME_FG3A', 'HOME_FG3_PCT', 'HOME_OREB', 'HOME_DREB', 'HOME_REB', 'HOME_AST', 'HOME_STL', 'HOME_BLK', 'HOME_TOV', 'HOME_PF', 'HOME_PTS']
away_stats.columns = ['GAME_ID', 'AWAY_TEAM_ID', 'AWAY_TEAM_NAME', 'AWAY_FGM', 'AWAY_FGA', 'AWAY_FG_PCT', 'AWAY_FTM', 'AWAY_FTA', 'AWAY_FT_PCT', 'AWAY_FG3M', 'AWAY_FG3A', 'AWAY_FG3_PCT', 'AWAY_OREB', 'AWAY_DREB', 'AWAY_REB', 'AWAY_AST', 'AWAY_STL', 'AWAY_BLK', 'AWAY_TOV', 'AWAY_PF', 'AWAY_PTS']

# Merge home and away team statistics for each game using the GAME_ID column
game_stats = pd.merge(home_stats, away_stats, on='GAME_ID')

# Sort the games by date
game_stats = game_stats.sort_values('GAME_ID')

# transform Win/Loss column to bool int value
game_stats['HOME_TEAM_WIN'] = game_stats['HOME_TEAM_WIN'].apply(lambda row: row == 'W').astype(int)

# save as csv
if not os.path.isdir("data"):
    os.mkdir("data")
game_stats.to_csv(f"data/{filename}")