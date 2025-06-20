# from bot.prizepicks import PrizePicks
# from bot.underdog import UnderDog
from bot.database import Database

# prizepicks = PrizePicks()
# underdog = UnderDog()

# props = prizepicks.current_props()
# print(props)


# props = underdog.current_props()
# print(props)

try:
    db = Database()
    data = db.table('prizepicks_lines')
    print(data)
except Exception as e:
    print(e)
finally:
    db.close_connection()
