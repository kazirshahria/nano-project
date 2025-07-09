import joblib
import warnings
import numpy as np
import pandas as pd
from thefuzz import process
from .database import Database

class Tools:
    warnings.filterwarnings('ignore')

    def __init__(self):
        self.model = joblib.load('./model.joblib')
        self.WEIGHTS = np.array([0.25, 0.20, 0.15, 0.125, 0.115, 0.10, 0.05, 0.01])
        self.WEIGHT_COLS = ["kills", "headshots", "assists", "deaths", "kast", "adr", "rating"]

        # Load encoders
        self.db = Database()
        self.team_mapper = self._load_mapper("teams_encoded", ["hltv_url", "map_number"])
        self.player_mapper = self._load_mapper("players_encoded", ["hltv_url", "map_number"])
        self.team_hltv_df = self._load_hltv_map("team_map", "team_id")
        self.player_hltv_df = self._load_hltv_map("player_map", "player_id")

        # Load and process data
        self.cs_data = self.db.table('hltv_cs')
        self.db.close_connection()

    def _load_mapper(self, table_name, index_cols):
        df = self.db.table(table_name)
        df.set_index(index_cols, inplace=True)
        return df.to_dict()["std"]

    def _load_hltv_map(self, table_name, index_col):
        df = self.db.table(table_name)
        df.dropna(inplace=True)
        df[index_col] = df[index_col].astype(int)
        df.set_index(index_col, inplace=True)
        return df

    def _agg_data(self, cs_data, map_df, map_three):
        stats = ["kills", "headshots", "assists", "deaths", "kast", "adr", "rating"]
        avg_stats = {"kast", "adr", "rating", "k_d_diff", "fk_diff"}
        num_maps = 3 if map_three else 2
        map_indices = range(1, num_maps + 1)

        for stat in stats:
            idx = cs_data.columns.get_indexer([stat])[0]
            total = sum(map_df[f"{stat}_map_{i}"] for i in map_indices)
            averaged = total / num_maps if stat in avg_stats else total
            map_df.insert(loc=idx, column=stat, value=averaged)

        drop_cols = [f"{stat}_map_{i}" for stat in stats for i in map_indices]
        map_df.drop(columns=drop_cols, inplace=True)

        map_df["map_number"] = f"MAPS 1-{num_maps}"
        return map_df

    def _get_data(self):
        cs_data = self.db.table('hltv_cs')
        self.db.close_connection()
        cs_data[["kast", "adr", "rating"]] = cs_data[["kast", "adr", "rating"]].astype(float)
        cs_data["date"] = pd.to_datetime(cs_data["date"])

        invalid_maps = {'Best of 3', 'Best of 2', 'All', 'Cache'}
        cs_data = cs_data[~cs_data["map"].isin(invalid_maps)].dropna().reset_index(drop=True)
        cs_data.drop(columns=["k_d_diff", "fk_diff", "event", "date", "map", "team", "opponent", "player_name", "team_score", "opponent_score"], inplace=True)

        cs_data = cs_data.groupby("match_url").filter(lambda g: set(g["map_number"]).issubset({1, 2, 3})).reset_index(drop=True)

        map_1 = cs_data[cs_data["map_number"] == 1]
        map_2 = cs_data[cs_data["map_number"] == 2]
        map_3 = cs_data[cs_data["map_number"] == 3]

        target_cols = ["match_url", "player_url", "kills", "headshots", "assists", "deaths", "kast", "adr", "rating"]
        map_1_2 = pd.merge(map_1, map_2[target_cols], on=["match_url", "player_url"], suffixes=("_map_1", "_map_2"))

        map_1_2_3 = map_1_2.merge(map_3[target_cols], on=["match_url", "player_url"]).rename(columns={
            "kills": "kills_map_3", "headshots": "headshots_map_3", "assists": "assists_map_3",
            "deaths": "deaths_map_3", "kast": "kast_map_3", "adr": "adr_map_3", "rating": "rating_map_3"
        })

        map_1_2 = self._agg_data(cs_data, map_1_2, map_three=False)
        map_1_2_3 = self._agg_data(cs_data, map_1_2_3, map_three=True)

        map_1 = map_1.copy()
        map_3 = map_3.copy()
        map_1["map_number"] = "MAPS 1"
        map_3["map_number"] = "MAPS 3"

        return pd.concat([map_1, map_3, map_1_2, map_1_2_3], ignore_index=True)

    def map_all_data(self, props: list, sportsbook: str = 'PP', odds: list = None):
        hltv_df = self.cs_data

        if len(props) == 0:
            print(f'{sportsbook} has no props.')
            return None
        
        def remove_words_in_team_name(word: str):
            team_name_split = word.split(' ')
            remove_words = ['esports', 'esport', 'sport', 'sports', 'team']
            team = " ".join([word for word in team_name_split if word.lower() not in remove_words])
            return team

        unique_teams = hltv_df['team'].unique()
        all_unique_players = hltv_df['player_name'].unique()
        prop_teams: list = []
        teams_detected: dict = {}
        props_matched = 0
        
        # Match player by finding the team first
        for prop in props:
            prop_team, prop_player = prop['Player Team'], prop['Player Name']
            
            # If either variables are None then continue onto the next one
            if prop_team is None or prop_player is None:
                continue
            
            # Later usage for odds
            if prop_team not in prop_teams:
                prop_teams.append(prop_team)

            prop_team = remove_words_in_team_name(str(prop_team))
            teams_matched = process.extract(prop_team, unique_teams, limit=5)
            
            # Narrow down the choices by locating the team first
            for team in teams_matched:
                df = hltv_df[hltv_df['team'] == team[0]]
                unique_players = df['player_name'].unique()
                players_matched = process.extractOne(prop_player, unique_players, score_cutoff=80)

                if players_matched is not None:
                    player_df = df[df['player_name'] == players_matched[0]]
                    player_values = player_df.tail().iloc[0]
                    player_url = player_values['player_url']
                    player_team = player_values['team_url']

                    # Add player team to a dictionary
                    team_not_found = teams_detected.get(prop_team)
                    if team_not_found == None:
                        teams_detected[prop_team] = team

                    props_matched += 1

                    prop.update(
                        {
                            'Player URL': player_url,
                            'Team URL': player_team
                        }
                    )
                    break
            
        # Find the best match using the name only
        risky_matches = 0
        for prop in props:
            player_url_not_found, player_name = prop.get('Player URL'), prop.get('Player Name')

            if player_url_not_found == None:
                best_players = process.extractBests(player_name, all_unique_players, limit=10)
                
                for best_player in best_players:
                    player_match_df = hltv_df[hltv_df['player_name'] == best_player[0]]
                    
                    # There's a 100% name match
                    if best_player[1] == 100:
                        prop.update(
                            {
                                'Player URL': player_match_df.iloc[0]['player_url']
                            }
                        )
                        risky_matches += 1
                        break

        # Opponent teams
        for prop in props:
            prop_opponent = prop['Opp']

            # Skip opponents that are none
            if prop_opponent is None:
                continue
            
            # Later usage for odds
            if prop_opponent not in prop_teams:
                prop_teams.append(prop_opponent)

            prop_opponent = remove_words_in_team_name(prop_opponent)
            team_exist = teams_detected.get(prop_opponent)
            if team_exist:
                best_opponent_team = team_exist
            else:
                best_opponent_team = process.extractOne(prop_opponent, unique_teams, score_cutoff=65)
            
            df = hltv_df[hltv_df['team'] == best_opponent_team[0]]
            prop.update(
                {
                    'Opp URL': df.tail().iloc[0]['team_url']
                }
            )
        
        # Bovado odds
        for odd in odds:
            team_1, team_2, team_1_odd, team_2_odd = odd.get('Team 1'), odd.get('Team 2'), odd.get('Odd 1'), odd.get('Odd 2')

            team_1 = remove_words_in_team_name(team_1)
            team_2 = remove_words_in_team_name(team_2)

            team_1_best = process.extractOne(team_1, prop_teams, score_cutoff=60)
            team_2_best = process.extractOne(team_2, prop_teams, score_cutoff=60)

            if team_1_best is None or team_2_best is None:
                continue

            for prop in props:
                player_team, opp_team = prop.get('Player Team'), prop.get('Opp')

                if (player_team  == team_1_best[0]) and (opp_team == team_2_best[0]):
                    prop.update(
                        {
                            'Odd': team_1_odd   
                        }
                    )
                    continue

                if (player_team  == team_2_best[0]) and (opp_team == team_1_best[0]):
                    prop.update(
                        {
                            'Odd': team_2_odd   
                        }
                    )
                    continue
        
        print(f'Located {round(props_matched/len(props), 2) * 100}% ({props_matched}/{len(props)}) of the props on {sportsbook}')
        print(f'{risky_matches} props are risky matches (inactive or change of team) on {sportsbook}')
        return props

    def previous_game_stats(self, player_url: str, map_type: str):
        player_values, l15_values, l15_avg, l10_avg = None, None, None, None

        if player_url == None:
            return None

        stat_target = map_type.split()[-1].strip().lower()
        map_label = "".join(map_type.split()[1].strip())
        
        df = self.cs_data # Load the HLTV data from the database

        if map_label == '1':
            player_df = df[
                (df['player_url'] == player_url) &
                ((df['map_number'] == 1))
            ]

        elif map_label == '2':
            player_df = df[
                (df['player_url'] == player_url) &
                ((df['map_number'] == 2))
            ]

        elif map_label == '3':
            player_df = df[
                (df['player_url'] == player_url) &
                ((df['map_number'] == 3))
            ]

        elif map_label == '1-2':
            player_df = df[
                (df['player_url'] == player_url) &
                ((df['map_number'] == 1) | (df['map_number'] == 2))
            ]
            player_df = player_df.groupby('match_url').filter(lambda col: len(col) == 2)

        elif (map_label == '1-3') or (map_label == '1-2-3'):
            player_df = df[
                (df['player_url'] == player_url) &
                ((df['map_number'] == 1) | (df['map_number'] == 2) | (df['map_number'] == 3))
            ]
            player_df = player_df.groupby('match_url').filter(lambda col: len(col) == 3)

        else:
            player_df = pd.DataFrame() # Empty dataframe
        
        if not player_df.empty:
            player_values = (
                player_df
                .sort_values(by='date')
                .groupby(['match_url'])[stat_target]
                .sum()
                .values[::-1]
            )

            l15_values = player_values[:15]
            l10_avg = np.mean(player_values[:10]) if len(player_values) >= 10 else np.mean(player_values)
            l15_avg = np.mean(l15_values) if len(l15_values) > 0 else 0

        return player_values, l15_values, l10_avg, l15_avg

    def probability(self, player_values: np.ndarray, line: float):
        
        if player_values is None or line is None:
            return None, None, None
        
        probability = (sum(float(line) <= player_values))/len(player_values)
        edge = probability - 0.50
        
        if 0 < edge:
            p = 'O'
        elif edge < 0:
            p = 'U'
        else:
            p = 'N'

        return probability, edge, p

    def pretty_dataframes(self, props: list, sportsbook: str, odds: list, sort_by_list: list):
        # Locate the Urls
        props = self.map_all_data(props, sportsbook, odds)
        data_list: list = []
        
        def fix_type_column(title_element: str):
            title_element = str(title_element).title()
            title_element = title_element.replace('Maps ', 'M')
            title_element = title_element.replace('Headshots', 'Hs')
            title_element = title_element.replace('1-2-3', '1-3')
            return title_element
        
        props_not_found = 0
        df_columns = ['Player', 'Team', 'Opponent', 'Type'] + [f'M{i}' for i in range(1, 16)] +\
        [sportsbook, 'L10 Avg', 'L10 Diff', 'L15 Avg', 'L15 Diff', 'Chance', 'Edge +/-', 'O/U', 'Odd', 'URL']

        if props:
            for prop in props:
                prop_dict: dict = {
                    col: None for col in df_columns
                }
                player_url, player_name, player_team, map_type, prop_line = prop.get('Player URL'), prop.get('Player Name'), prop.get('Player Team'), prop.get('Type'), float(prop.get('Line Score'))
                
                prop_dict['Player'] = player_name
                prop_dict['URL'] = player_url
                prop_dict['Team'] = player_team
                prop_dict['Opponent'] = prop.get('Opp')
                prop_dict['Type'] = fix_type_column(map_type)
                prop_dict[sportsbook] = prop_line

                if player_url == None:
                    props_not_found += 1
                    data_list.append(prop_dict)
                    continue

                player_values, l15_values, l10_avg, l15_avg = self.previous_game_stats(player_url, map_type)

                if isinstance(l15_values, np.ndarray):
                    for k, v in enumerate(l15_values, start=1):
                        prop_dict[f'M{k}'] = v
        
                probability, edge, p = self.probability(player_values, prop_line)
                prop_dict['Chance'] = probability
                prop_dict['Edge +/-'] = 0 if probability == 0 else edge
                prop_dict['O/U'] = p
                
                prop_dict['L10 Avg'] = l10_avg
                prop_dict['L10 Diff'] = l10_avg - prop_line if pd.notnull(l10_avg) and pd.notnull(prop_line) else None
                prop_dict['L15 Avg'] = l15_avg
                prop_dict['L15 Diff'] = l15_avg - prop_line if pd.notnull(l15_avg) and pd.notnull(prop_line) else None
                prop_dict['Odd'] = prop.get('Odd')
                data_list.append(prop_dict)
        
        df = pd.DataFrame(data_list)
        
        # Organize the data
        if not df.empty:
            df.sort_values(by=sort_by_list, inplace=True)

        print(f'{props_not_found} props not found on {sportsbook}')
        return df

    def match_props_dataframe(self, pp_df: pd.DataFrame, ud_df: pd.DataFrame):
        df_columns = ['Player', 'Team', 'Opponent', 'Type'] + [f'M{i}' for i in range(1, 16)] +\
        ['PP', 'UD', 'PP-UD', 'PP Chance', 'PP O/U', 'UD Chance', 'UD O/U', 'Edge +/-', 'Odd', 'URL']

        props_mapped = 0
        mapped_props_list: list = []
        
        unique_teams = list(set(list(pp_df['Team'].values) + list(pp_df['Opponent'].values)))
        pp_props = pp_df.to_dict(orient='records')
        ud_props = ud_df.to_dict(orient='records')

        # Loop through Prizepicks
        for pp_prop in pp_props:
            pp_player_url, pp_opponent_team, pp_stat_type = pp_prop.get('URL'), pp_prop.get('Opponent'), pp_prop.get('Type')

            if pp_player_url is None or pp_opponent_team is None or pp_stat_type is None:
                continue

            for ud_prop in ud_props:
                ud_player_url, ud_opponent_team, ud_stat_type = ud_prop.get('URL'), ud_prop.get('Opponent'), ud_prop.get('Type')
                
                if ud_player_url is None or pp_opponent_team is None or ud_stat_type is None:
                    continue
                
                # Fuzzy match
                ud_opponent_team_best = process.extractOne(ud_opponent_team, unique_teams)
                
                
                if (pp_player_url.strip() == ud_player_url.strip()) & (pp_opponent_team.strip() == ud_opponent_team_best[0].strip()) & (pp_stat_type.strip() == ud_stat_type.strip()):
                    props_mapped += 1
                    prop_dict: dict = {}

                    # Update the Prizepicks prop with UD information
                    for df_column in df_columns:
                        if df_column == 'UD':
                            prop_dict[df_column] = ud_prop.get('UD')
                        elif df_column == 'PP-UD':
                            prop_dict[df_column] = float(pp_prop.get('PP')) - float(ud_prop.get('UD'))
                        elif df_column == 'PP Chance':
                            prop_dict[df_column] = pp_prop.get('Chance')
                        elif df_column == 'PP O/U':
                            prop_dict[df_column] = pp_prop.get('O/U')
                        elif df_column == 'UD Chance':
                            prop_dict[df_column] = ud_prop.get('Chance')
                        elif df_column == 'UD O/U':
                            prop_dict[df_column] = ud_prop.get('O/U')
                        elif df_column == 'Edge +/-':
                            prop_dict[df_column] = ((pp_prop.get('Chance') + ud_prop.get('Chance')) / 2) - 0.50
                        else:
                            prop_dict[df_column] = pp_prop.get(df_column)
                    mapped_props_list.append(prop_dict)
                    break
        
        print(f'{props_mapped} matches found of PP & UD props')
        return pd.DataFrame(mapped_props_list).sort_values(by='PP-UD')

