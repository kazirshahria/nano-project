import os
from gspread import service_account
import pandas as pd
import numpy as np

class GoogleSheet(object):
    
    def __init__(self, sheet_url: str = os.environ['GSHEET_URL']):
        service_account_login = service_account(filename='google_credentials.json')
        self.client = service_account_login.open_by_url(sheet_url)
    
    def worksheet_instance(self, id: str = '635725027'):
        return self.client.get_worksheet_by_id(id=id)
    
    def update_worksheet(self, worksheet, df: pd.DataFrame):
        df.fillna('', inplace=True)
        df = df.astype(str)
        
        if not df.empty:
            worksheet.clear()
            return worksheet.update([df.columns.values.tolist()] + df.values.tolist(), value_input_option="USER_ENTERED")