import os
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
import gspread
from gspread_dataframe import set_with_dataframe
import pandas as pd

# Load credentials from environment variable
creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

credentials = service_account.Credentials.from_service_account_info(creds_dict, scopes=scopes)

# Build services
service = build('sheets', 'v4', credentials=credentials)
gc = gspread.authorize(credentials)

# Spreadsheet ID and ranges
SPREADSHEET_ID = '1hZoXvTlCsIbHDn1bRjME3ycVjUQf1uLFKW7wPTdac6w'
range_name = 'Sheet1!A1:C10'

def read_sheet(spreadsheet_id, range_name):
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=range_name).execute()
    return result.get('values', [])

# Read and write data
data = read_sheet(SPREADSHEET_ID, range_name)
df = pd.DataFrame(data[1:], columns=data[0])
print(df)

sh = gc.open_by_key(SPREADSHEET_ID)
wks = sh.worksheet('Sheet2')
set_with_dataframe(wks, df, include_index=False, include_column_header=True)
