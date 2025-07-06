import pandas as pd
from datetime import datetime
from thefuzz import process
from .utils.bot_utils import json_response

class Bovado:
    def __init__(self):
        self.url = 'https://www.bovada.lv/services/sports/event/coupon/events/A/description/esports/counter-strike-2?marketFilterId=def&preMatchOnly=true'
        pass

    def current_odds(self):
        
        headers = {
        'accept': 'application/json, text/plain, */*',
        'referer': 'https://www.bovada.lv/',
        'user-agent': 'Mozilla/5.0 (Windows; Windows NT 6.3; Win64; x64; en-US) AppleWebKit/603.39 (KHTML, like Gecko) Chrome/48.0.3194.329 Safari/535',
        }

        odds_dict: dict = {}

        response = json_response(self.url, headers)
        
        for data in response:
            events = data['events']

            for event in events:

                id = event['id']
                matchup = event['description']
                time_stamp = event['startTime']/1000
                date_only = datetime.fromtimestamp(time_stamp).date()

                # If the ID is not in the dictionary
                if not odds_dict.get(id):
                    odds_dict[id] = {
                        'Matchup': matchup.split(' vs '),
                        'Date': date_only
                    }
                
                markets = event['displayGroups'][0]['markets']

                for market in markets:
                    if market['description'] == 'Moneyline':
                        lines = market['outcomes']
                        
                        for i, line in enumerate(lines, start=1):

                            team = line['description']
                            american_odd = line['price']['american']
                            odds_dict[id][f'Team {i}'] = team
                            odds_dict[id][f'Odd {i}'] = american_odd
        values = odds_dict.values()
        return values
    
