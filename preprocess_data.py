import pandas as pd

class Preprocess:
    def __init__(self) -> None:
        self.games = pd.read_csv(f"GamesRegularSeason_10years.csv")
        self.playoffs = pd.read_csv(f"GamesPlayoffs_10years.csv")
        self.spec_players = pd.read_csv("players_specs_10years.csv")        
        print(f"Number of teams : {len(self.games['HOME_TEAM_ID'].unique())}") # we can have more than 30 teams 

    def build_dataframe_for_year(self,year,playoffs):
        """Build in a dataframe a dataset where each column represents a game.
        For each game, we store the statistics of each player that played."""        
        
        year_id = str(year) + "-" + str(year+1)[-2:]
        spec_players = self.spec_players.loc[self.spec_players["SEASON_ID"] == year_id]
        cols_for_spec_players = ['PLAYER_ID', 'PLAYER_AGE', 'MIN', 'FGM', 'FGA', 'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT', 'FTM', 'FTA', 'FT_PCT', 'OREB',
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
        col_players = ["PLAYER_ID","GAME_ID","TEAM_ID","MIN","TEAM_ABBREVIATION"]

        # convert MIN as "minutes:secondes" to minutes as floats
        players['MINUTES'], players['SECONDS'] = players['MIN'].str.split(':', 1).str
        players['MINUTES'] = pd.to_numeric(players['MINUTES'])
        players['SECONDS'] = pd.to_numeric(players['SECONDS'])
        players['MIN'] = players['MINUTES'] + players['SECONDS'] / 60
        # drop the original MINUTES and SECONDS columns
        players = players.drop(['MINUTES', 'SECONDS'], axis=1)

        # only keep the 8 players of each team that played the most
        top_8_players = players[col_players].groupby(['GAME_ID', 'TEAM_ID']).apply(lambda x: x.sort_values('MIN', ascending=False).head(8))
        top_8_players = top_8_players.reset_index(drop=True)
        top_8_players.drop("MIN",axis=1,inplace=True) # not needed anymore

        # merge spec players and players that played
        stats = pd.merge(spec_players,top_8_players,on="PLAYER_ID")
        stats.sort_values(["GAME_ID","PLAYER_ID"])


        # retrieve data we need from games dataframe
        group_by = ["GAME_DATE","GAME_ID","HOME_TEAM_ID","AWAY_TEAM_ID","HOME_TEAM_WIN"]
        if playoffs:
          game_specs = self.playoffs[group_by]
        else:
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
        dataset.drop(columns,axis=1,inplace=True)

        # fill all NaN with zeros
        dataset.fillna(0,inplace=True)
        
        return dataset
    
    def dataset_for_n_years(self, n_years : int, include_playoffs = False):
        dfs = []
        for year in range(2023 - n_years,2023):
            dataset = self.build_dataframe_for_year(year,include_playoffs)
            dfs.append(dataset)
        concat = pd.concat(dfs)
        concat.fillna(0,inplace=True)
        return concat