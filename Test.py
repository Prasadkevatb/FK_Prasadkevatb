from google.oauth2 import service_account
from googleapiclient.discovery import build
import time
import gspread
from gspread_dataframe import set_with_dataframe
import io
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import pandas as pd
from io import StringIO
from google.colab import files


cread = {
  "type": "service_account",
  "project_id": "prasadk-239402",
  "private_key_id": "b1f63d9b7cbb4fd31d149ecb8f9f35e8992437a5",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCym36BNlq+S03t\nPcvmDx29Cp0LajqWqTPEbVgpxJz6id6C57OkEo4uZZdIvviwAYOfxMhf+WlwS9NX\n6gzJRtkdQV3R+c48+GFu9F70RS71LNf6EseXQ2h9nSkL/Wr49MxenyuHQtEirGsA\nGYs6P+Bos/1YiADGvCGigL12BJCfuukBSWrZW70CzC8/mCHigu+R1HuwXfV9XQKS\nurqByU5uoLn4kKdbh6i0TQ3ZPdV/vSnF9mM+2fSovdxIQKhnrdUSc4IYgMlL3xw3\n0ifu3GfKMncNaJQt1FbnVBjf2GQCQ4He0a3TsVWcqMR3HkHRi+eDIlb2N0h06ySy\nLmPiCOXLAgMBAAECggEABSsjw4h3oscaF7HwmxU35pcOiVyHGoIF+fqyEO9cHZHs\nyvv6glZ5H9WXxaalGq7IiNCQfdqBVxsSoBopSY/Py51vIhrpAXGsnCHdN5Ni8vxb\nuaReez2bofrwy6SHOnIXEevoPg9MbwTvSb7zfPmJPG5s9+ljoFykWogAM5CZQM4C\n0ffgjZ5ExclmlDEsP9uabn9c6i0kfVkmmWEov77NdMA/eL3OghO5iolCmPuiSIci\nz61/MM8YSorCa0YqWh6ljg5EdiMSE2b3NBa3DsvIW/dHAssAjx6FMKMS0/JRL/oq\nPdQqQTXnTi9X+aMWDDm645U4Q3/DU3kW2+NEK8fs9QKBgQD8ETmkQRWCLMdTnQDi\nlWy2tu43ydcYOJ7tRJuo8DXKdihm7Ig1+JQN0lxA4JsRpW1YbEPt5/IxqRZWGO4L\nCpVp7tOj5xxPgeYoiGNW0V/2dh0LtUEpVEFNY8e6hA5cqO98xy1KTPRe3tUUB7LL\nyZDoYqCt0BaXlfBeOPHhKvKlfQKBgQC1ZN1sW44OjaSy7KnzXh1nnSvcsuMawXM9\nqWd4k1L3GyI9uIHC3bCAUpzjnbEa5YbHUbD9xL62bTAzxYzYCzoS5hhXFdniaAju\n9BWBETlVfOX5nAzHGJoVCK0KMT/JFLU/VuU87/AfBdJRVy59exXpJkvSfhEw8L3a\nHfgh7Jh65wKBgQDbFB2FJQwMl96mTU73n+dc5qEk28iWxJ9cmMSxkBUwYoG68tlw\nDxye5rZHrO8Z5y2iLHbdzzow70T6j7BU6F30NB691aBFeiEQGXo9erxs+TtFccOw\nqAoZuR9efGf/INUFHhe+/CNoUUPgpNBBTm8jipUcfD8mgKrpOZUAntNGAQKBgGIp\nxZIAMe46ROj3HjmvsuYBrlzvCevOyJiT9oTP5VQIgQ05ri9QXVX0XybmjZNqpvdy\ng/+w2yxKBo3d2IyJ9tGHZ6CpGJJnjn4R5RlFwus6fhIImvmbnLbJTSt433XoGPXA\nBqplhmKjed+++E+7rm3P5bRxuMftYDCLi16AdTv9AoGBALcokuAwNWjzAOjlCDMq\nsophkKV5luyCipDNZMh1DSNm5W/OGqVdqotqe6sJeqt8bO8cGIf2Xc3tX/KIGbh2\nJLb11gSn7HOJcA4jsF5hpZFs3nj5DF550cWcewI+SHYUMKppEuPH1pwrK2h4HG79\nTYAHUYC7YBI994YaOraBTVQM\n-----END PRIVATE KEY-----\n",
  "client_email": "prasadk-239402@prasadk-239402.iam.gserviceaccount.com",
  "client_id": "109242629213306538083",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/prasadk-239402%40prasadk-239402.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}

scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

credentials = service_account.Credentials.from_service_account_info(cread, scopes=scopes)

service = build('sheets', 'v4', credentials=credentials)

SPREADSHEET_ID = '1hZoXvTlCsIbHDn1bRjME3ycVjUQf1uLFKW7wPTdac6w'

def read_sheet(spreadsheet_id, range_name):
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=range_name).execute()
    values = result.get('values', [])
    return values

range_name = 'Sheet1!A1:C10'
data = read_sheet(SPREADSHEET_ID, range_name)

df = pd.DataFrame(data[1:], columns=data[0])
print(df)


gc = gspread.authorize(credentials)
sh = gc.open_by_key(SPREADSHEET_ID)
wks = sh.worksheet('Sheet2')

set_with_dataframe(wks, df, include_index=False, include_column_header=True)
