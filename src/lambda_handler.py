from bot.tools import Tools
from bot import pp, ud, gs, bv

def run():
    print("Starting Google Sheet update...")

    pp_props = pp.current_props()
    ud_props = ud.current_props()
    odds = bv.current_odds()

    tools = Tools()

    df_1, df_1a = tools.pretty_dataframes(props=pp_props, sportsbook='PP', odds=odds, sort_by_list=['Team', 'Opponent', 'Player'])
    df_2, df_2a = tools.pretty_dataframes(props=ud_props, sportsbook='UD', odds=odds, sort_by_list=['Team', 'Opponent', 'Player'])
    df_3 = tools.match_props_dataframe(df_1, df_2)
    df_4 = tools.previous_props_dataframe(props=pp_props, sportsbook='PP', days=7)
    df_5 = tools.previous_props_dataframe(props=ud_props, sportsbook='UD', days=7)
    df_6 = tools.last_update_dataframe()

    # GSheet updates
    sheet_ids = [
        ('88012551', df_1),
        ('1121328359', df_2),
        ('686271289', df_3),
        ('2072968799', df_4),
        ('1498038724', df_5),
        (2040151409, df_6),
        ('1359208711', df_1a),
        ('275648232', df_2a),
    ]

    for sheet_id, dataframe in sheet_ids:
        wi = gs.worksheet_instance(sheet_id)
        gs.update_worksheet(wi, dataframe)

    print("Google Sheet update complete.")

def handler(event=None, context=None):
    print("Lambda invoked")
    try:
        run()
        return {"statusCode": 200, "body": "Update complete"}
    except Exception as e:
        print(f"Lambda failed: {e}")
        return {"statusCode": 500, "body": str(e)}