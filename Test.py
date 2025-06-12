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
  "private_key_id": "e8d38a1b4c3819901a36f80579248db17a782008",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCnyhXd5djEAd22\ngzZ0f+ubX77EUKIdA+ihwKTETgsnW+rlPBrm7+JHnyZc8EHncl5boUAnMyubod/D\nTJ7XRpZ6oM0+RxiUUfk40JtQkiGYLduIBbNUJ9InWFYMBChdCO0VCmAoOiMUaaqy\nFfW2bFM7BFHE8W3KW99JDKNDMxhwBH8/99+nRwq0RJBvkglntVUUT5jje919ULNx\nId+9EtRMrH9ugiqBU/dAMGn2fFQZY9qXeTxxh2cXG8NSiDpWGWdkdggRaqTp6jq+\nDJyFPfWPixFms4Gka4UYd0FGGBACn9XcOkmMXN2x5uW8EKDCq1CLyQjxtTz5jIBQ\nTJWEvwM/AgMBAAECggEARJlAe2o/R1Oj+7yFhPfPscRhUA3PWaGBeLA0LPXjZYIU\n9Qevz/7/Olz86D5qE2AU581zVxN6nrR9sXEf98+qMhSQFQgoKmuhQvM2rKgyB++n\ns2LxQFTPfLqG437HqdG+bD3Dtm6ebPgOi6SiwqRjGtvXOneidXs+PLVoAk9fjFwA\noDCCSWMNULXIRFrRluOV2hgocmrkCrgI0vuOpaiQFdkO2b/l4/51GO24u4lU5TjJ\nSpDSLz3kVubylNw+3uBNPMh245lYGk60GOlVrW1rieLvklf8sAeKH6SCpuou/w8W\nBy/OkP723qgkbee7MlDsPc0OP7OT0VFspTIueIue8QKBgQDaRxzquwBpDPdc660H\n+TJOlZopALGl8VF30HFK8VqGGFwlnNJM9Km5f2JM2P2+zkfY07HwCwr/v5NmMf9S\ns5odJg5G0/5xdeTUZbkBaT1RY2K4636iFX9WKC9+X45U3jBfHgdUDXpnSAOBrHeS\n4H4HVDLi3nGMQCJiNCPRDZWGywKBgQDEyU1HpU/zJ5ZXGCsKKsxbkfhjkwlKZE6x\nBa0H0aHRIjT6/mTxhOdNaLmc/4MgftIbwkSRelbXAYMJkPtebFTxpSzOtgQKwUBA\nOgttLwsfbjssRT9kcL9o+s2dh4CPRp7ESJ/ctDPZGEjAn4/LiZZqw+PEnQVmf0/h\ns4fpwkIy3QKBgCMLa88b+vLizAw1InC3R6ZqSnfuzpbP6b6MzsOzgE8rNhAr32/P\nkDhiBUxFQmgSAmMDifv6KefpwCaWPGHwx/uKEpy4iI962CRpuIxLczbP+BesZ4Zq\nYSlaBlYJzXY+vhFnZtHN4CsY2sdFA2WneVR/5jOadyyHV33g1SdJQQqFAoGBAKvd\n/Ga0lM0TKBTNANoSYwUeycd9pcGZNbkZ8fNAPYm/zHHiORtPLgzHiET8S/PXj326\n/9Pm+20w3tc+Dqx7BP0/e5DWG1WhTpJJn60uRxKeXYPevuzkI+h2E5qfu8JmtUdM\n/oHtyNK0iOXi9d2YPwzkJUuUsCDC1GuVNwEMLnDpAoGARejO+BzgJ+LZd9wep2Mx\nx/b4wfKb/Mg7a5alazSoVjGfJmt7JvXUfGSB5jYtVSltSolJWPWhbEbJjvYtVt1x\nwRDzH+RtTDHYF+g8PjWuFCJwc4O/nW8ldwLQt2vNVmNTqdknwQA8VqrHneMKuU/E\n+tBz3G9iCAaAHS4egDZDTyw=\n-----END PRIVATE KEY-----\n",
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
