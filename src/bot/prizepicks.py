from .utils.bot_utils import json_response
from .database import Database
from thefuzz import process
from datetime import datetime
from pandas import DataFrame

class PrizePicks():

    def __init__(self):
        ...

    def current_props(self, league: str = '265') -> list:
        '''
        Fetches for the current props displayed on Prizepicks on any given league.

        To search other leagues, please refer to the GitHub file [here](#https://github.com/kazirshahria/nano-project/tree/master/data/leagues.csv).
        The file has the unique id's for leagues that Prizepicks supports on their platform.

        ---

        ## Parameters:
            **league**: *str*
            The unique identifier for a league on PrizePicks.

        ## Returns:
            **props**: *list*
            A list of dictionaries, each containing details about a line.
        '''
        url = f'https://partner-api.prizepicks.com/projections?league_id={league}'
        response: dict = json_response(url)

        player_mapper = {}
        players = response.get('included')

        if players is None:
            return []

        for player in players:
            if player['type'] == 'new_player':
                id = player.get('id')
                name = player['attributes']['display_name'].strip()
                team = player['attributes']['team'].strip()
                team_id = player['relationships']['team_data']['data']['id'].strip()
                if id not in player_mapper.keys():
                    player_mapper[id] = {
                        'Name': name,
                        'Team': team,
                        'Player ID': id,
                        'Team ID': team_id
                    }

        prop_list: list = []
        lines = response.get('data')

        line: dict
        for line in lines:
            line_id = line['id']

            opp = line['attributes']['description'].replace('MAPS', 'MAP')\
                .replace('MAP', 'MAPS').split('MAPS')[0]

            date = datetime.fromisoformat(line['attributes']['start_time'])
            player_id = line['relationships']['new_player']['data']['id']
            line_score = line['attributes']['line_score']
            stat_type = line['attributes']['stat_type']

            player_info = player_mapper.get(player_id)
            name = player_info.get('Name')
            team = player_info.get('Team')
            player_id = player_info.get('Player ID')
            team_id = player_info.get('Team ID')

            prop_list.append(
                {
                    'ID': line_id,
                    'Game Date': date.date(),
                    'Game Time': date.time(),
                    'Type': stat_type.replace('MAP 3', 'MAPS 3'),
                    'Player Name': name.strip(),
                    'Player Team': team.strip(),
                    'Opp': opp.strip(),
                    'Line Score': line_score,
                    'Player ID': player_id,
                    'Team ID': team_id,
                }
            )

        return prop_list