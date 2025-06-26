from .utils.bot_utils import json_response
from datetime import datetime


class UnderDog(object):

    def __init__(self):
        pass

    def current_props(self):
        """
        Fetches and returns the current props displayed on Underdog Fantasy.
        """

        def fetch_teams(url: str = 'https://stats.underdogfantasy.com/v1/teams'):
            teams = json_response(url)["teams"]
            return {team.pop("id"): {"abbr": team.get("abbr"), "name": team.get("name")} for team in teams}

        def fix_map_name(map_name):
            if "on" in map_name:
                parts = map_name.split("on")
                if len(parts) == 2:
                    swapped = f"{parts[1]} {parts[0]}"
                    return swapped.replace("Map 1", "Maps 1").replace("+", "-").replace("1+2+3", "1-3")
            return map_name

        url = "https://api.underdogfantasy.com/beta/v5/over_under_lines"
        lines = json_response(url)
        teams = fetch_teams()

        players = lines["players"]
        games = lines["games"]
        appearances = lines["appearances"]
        over_under_lines = lines["over_under_lines"]

        data = []

        for player in players:
            if player["sport_id"] != "CS":
                continue

            pl_name = player["last_name"]
            pl_tm_id = player["team_id"]
            pl_id = player["id"]

            for appearance in appearances:
                if appearance["player_id"] != pl_id:
                    continue

                appearance_id = appearance["id"]
                match_id = appearance["match_id"]

                for game in games:
                    if game["id"] != match_id:
                        continue

                    home_tm_id = game["home_team_id"]
                    away_tm_id = game["away_team_id"]
                    scheduled_at = datetime.strptime(game["scheduled_at"], "%Y-%m-%dT%H:%M:%SZ")
                    team_names = game["title"]

                    for line in over_under_lines:
                        appearance_stat = line["over_under"]["appearance_stat"]
                        if appearance_stat["appearance_id"] != appearance_id:
                            continue

                        stat_type = fix_map_name(str(appearance_stat["display_stat"]))
                        options = line["options"]
                        line_id = line["id"]
                        stat_value = line["stat_value"]

                        over_odd = under_odd = 0
                        if len(options) == 2:
                            for option in options:
                                if option["choice_display"] == "Higher":
                                    over_odd = option["american_price"]
                                else:
                                    under_odd = option["american_price"]

                        pl_tm_name = teams.get(pl_tm_id, {}).get("name") if pl_tm_id else None
                        opp = (
                            team_names.replace(pl_tm_name, "").replace("vs", " ").strip()
                            if pl_tm_name else None
                        )
                        opp_id = (
                            away_tm_id if pl_tm_id == home_tm_id
                            else home_tm_id if pl_tm_id else None
                        )

                        data.append({
                            "ID": line_id,
                            "Game Date": scheduled_at.date(),
                            "Game Time": scheduled_at.time(),
                            "Stat Type": stat_type,
                            "Player Name": pl_name,
                            "Player Team": pl_tm_name,
                            "Opp": opp,
                            "Matchup": team_names,
                            "Line Score": stat_value,
                            "Over Odd": over_odd,
                            "Under Odd": under_odd,
                            "Player ID": pl_id,
                            "Team ID": pl_tm_id,
                            "Opp ID": opp_id,
                            "Home Team ID": home_tm_id,
                            "Away Team ID": away_tm_id,
                        })

        return data