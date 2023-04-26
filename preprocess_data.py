import pandas as pd
import numpy as np

class Preprocess:
    def __init__(self, number_of_years : int) -> None:
        self.number_of_years = number_of_years
        self.games = pd.read_csv("games10years.csv")
        assert len(self.games['HOME_TEAM_ID'].unique()) == 30
        self.spec_players = pd.read_csv("players_specs_10years.csv")        


    def build_dataframe_for_year(self,year):
        """Build in a dataframe a dataset where each column represents a game.
        For each game, we store the statistics of each player that played."""        
        
        year_id = str(year) + "-" + str(year+1)[-2:]
        spec_players = self.spec_players.loc[spec_players["SEASON_ID"] == year_id]
        cols_for_spec_players = ['PLAYER_ID', 'PLAYER_AGE', 'GP', 'GS', 'MIN', 'FGM', 'FGA', 'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT', 'FTM', 'FTA', 'FT_PCT', 'OREB',
       'DREB', 'REB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS', 'HEIGHT','WEIGHT', 'SEASON_EXP']
        spec_players = spec_players[cols_for_spec_players]
        
        # convert FEET and INCHES to cm
        spec_players['FEET'], spec_players['INCHES'] = spec_players['HEIGHT'].str.split('-', 1).str
        spec_players['FEET'] = pd.to_numeric(spec_players['FEET'])
        spec_players['INCHES'] = pd.to_numeric(spec_players['INCHES'])
        spec_players['HEIGHT_CM'] = spec_players['FEET'] * 30.48 + spec_players['INCHES'] * 2.54
        # drop the original FEET and INCHES columns
        spec_players = spec_players.drop(['HEIGHT', 'FEET', 'INCHES'], axis=1)
        
        players = pd.read_csv(f"players_data_{year}.csv")
        col_players = ["PLAYER_ID","GAME_ID","TEAM_ID", 'TEAM_ABBREVIATION', 'TEAM_CITY', 'PLAYER_NAME','NICKNAME', 'START_POSITION', 'COMMENT']

        # merge spec players and players that played
        stats = pd.merge(spec_players,players[col_players],on="PLAYER_ID")
        stats.sort_values(["GAME_ID","PLAYER_ID"])

        # columns not needed
        stats.drop(['GP','TEAM_CITY','PLAYER_NAME', 'NICKNAME', 'START_POSITION', 'COMMENT'],axis=1,inplace=True)

        # retrieve data we need from games dataframe
        group_by = ["GAME_DATE","GAME_ID","HOME_TEAM_ID","AWAY_TEAM_ID","HOME_TEAM_WIN"]
        game_specs = self.games[group_by]
        # merge on GAME_ID, to form a dataset with stats from each player that played during GAME_ID
        game_stats_merge = pd.merge(stats,game_specs,on="GAME_ID")

        # create a new dataframe with the players grouped by the home team
        home_players = game_stats_merge[game_stats_merge['TEAM_ABBREVIATION'] == game_stats_merge['HOME_TEAM_ID']] \
            .groupby(group_by) \
            .agg(list) \
            .reset_index()

        # create a new dataframe with the players grouped by the away team
        away_players = game_stats_merge[game_stats_merge['TEAM_ABBREVIATION'] != game_stats_merge['HOME_TEAM_ID']] \
            .groupby(group_by) \
            .agg(list) \
            .reset_index()

        # merge the two dataframes
        dataset = pd.merge(home_players, away_players, on=group_by, suffixes=("_HOME","_AWAY"))
        # drop columns not needed
        cols_to_drop = ["TEAM_ID","TEAM_ABBREVIATION"]
        cols_final_to_drop = [col + "_HOME" for col in cols_to_drop] + [col + "_AWAY" for col in cols_to_drop]
        dataset.drop(cols_final_to_drop,axis=1,inplace=True)


        # columns now contain lists of stats for each player, we separate into new columns here
        excluded_columns = ['GAME_DATE', 'GAME_ID', 'HOME_TEAM_ID', 'AWAY_TEAM_ID', 'HOME_TEAM_WIN','PLAYER_ID_HOME','PLAYER_ID_AWAY']
        # don't separate for these columns, they either have non list values, or are columns we don't need anymore
        columns = list(dataset.columns)
        for col in columns:
            if col in excluded_columns:
                continue
            dataset = dataset.join(dataset[col].apply(pd.Series).add_prefix(col + "_"))
        
        # remove all previous columns, since we won't feed them to the model
        columns.remove('HOME_TEAM_WIN') # we keep HOME_TEAM_WIN, giving the labels for the model
        columns.remove('GAME_DATE') # we keep GAME_DATE in order to separate playoffs games to regular season games
        dataset.drop(columns,axis=1,inplace=True)

        # fill all NaN with zeros
        dataset.fillna(0,inplace=True)
        
        return dataset