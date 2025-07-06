from bot.googlesheet import GoogleSheet
from bot.prizepicks import PrizePicks
from bot.underdog import UnderDog
from bot.bovado import Bovado
from bot.tools import Tools

pp = PrizePicks()
ud = UnderDog()
gs = GoogleSheet()
bovado = Bovado()
tools = Tools()

pp_props = pp.current_props()
ud_props = ud.current_props()
odds = bovado.current_odds()

df_1 = tools.pretty_dataframes(pp_props, 'PP', odds)
df_2 = tools.pretty_dataframes(ud_props, 'UD', odds)

wi_1 = gs.worksheet_instance('88012551')
wi_2 = gs.worksheet_instance('2125078460')

gs.update_worksheet(wi_1, df_1)
gs.update_worksheet(wi_2, df_2)

