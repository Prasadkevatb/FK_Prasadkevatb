import requests
import time
import io
import pandas as pd
import logging
from datetime import datetime, timedelta
from gspread_dataframe import set_with_dataframe
from google.oauth2 import service_account
from googleapiclient.discovery import build
import gspread

# --- 1. CONFIGURATION ---
AUTH_USER = "prasadkevatb.vc" 
CLIENT_ID = "scp_fsg_nrt"
CLIENT_SECRET = "04e4a7ec-9193-44b3-8b38-a41a69781820"
VALIDATOR_URL = "http://validator.fdp-qaas-validator-prod.fkcloud.in/query/queries"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_full_dataframe(sql):
    headers = {
        "content-type": "application/x-www-form-urlencoded",
        "x-client-id": CLIENT_ID,
        "x-client-secret": CLIENT_SECRET,
        "x-authenticated-user": AUTH_USER
    }
    data = {'query': sql, 'sourceName': 'BIGQUERY'}
    
    try:
        logging.info("Submitting SQL query to FDP...")
        response = requests.post(VALIDATOR_URL, headers=headers, data=data)
        if response.status_code != 200:
            logging.error(f"Server Response Error: {response.text}")
        response.raise_for_status()
        
        handle = response.json()['queryHandle']['handle']
        status_url = f"{VALIDATOR_URL}/{handle}/status"
        
        logging.info(f"Query Handle: {handle}. Waiting for completion...")
        while True:
            status_res = requests.get(status_url, headers=headers).json()
            query_status = status_res.get('status')
            
            if query_status == 'SUCCESSFUL':
                logging.info("Success! Downloading data...")
                dl_res = requests.get(status_res.get('signedUrl'))
                return pd.read_csv(io.StringIO(dl_res.text))
            elif query_status in ['FAILED', 'CANCELLED']:
                logging.error(f"Query Failed: {status_res}")
                return None
            time.sleep(15) 
    except Exception as e:
        logging.error(f"Logic Error: {e}")
        return None

# --- 2. DYNAMIC DATE & FSN SETUP ---
yesterday = datetime.now() - timedelta(days=1)
d_minus_1 = yesterday.strftime('%Y%m%d')

ten_days_ago = yesterday - timedelta(days=9) 
start_date = ten_days_ago.strftime('%Y%m%d')

# List of FSNs
fsn_list = ('MSCESXX8ENDF6NGS',
'MDMGG433JPPZEVFF',
'MDMETGMUEHP2YJDZ',
'MSCHDXPDR7BKCF3Y',
'MDMETGN5U9FHGNWE',
'MSCH3KBJZ75M93XF',
'MDMFGWB5UZKKZMXV',
'MDMETGN5BMJGXEXR',
'MSOEMQZ38RVJXF5X',
'MSCEYAVKGFEPR3QD',
'DPRGMXERGKN2QF4R')

# --- 3. FINAL CLEAN SQL ---
sql_query = f"""
SELECT
    CONCAT(SUBSTRING(CAST(cf_fact.actual_reservation_date_key AS STRING), 7, 2), '-', 
           SUBSTRING(CAST(cf_fact.actual_reservation_date_key AS STRING), 5, 2), '-', 
           SUBSTRING(CAST(cf_fact.actual_reservation_date_key AS STRING), 1, 4)) AS order_date,
    fr_dr_cluster_type,
    destination_cluster AS cluster_code,
    cluster_city,
    bu,
    super_category,
    vertical,
    fsn,
    is_first_party_seller AS seller_type,
    SUM(destination_cluster_sale) AS total_sales,
    SUM(source_cluster_sale) AS cf_sales,
    SUM(zf_sales) AS zf_sales
FROM bigfoot_external_neo.retail_ip__cluster_fulfillment_hive_fact cf_fact
WHERE
    bu IN ('BGM')
    AND actual_reservation_date_key >= {start_date}
    AND actual_reservation_date_key <= {d_minus_1}
    AND is_first_party_seller IS TRUE
    AND fsn IN {fsn_list}
GROUP BY
    actual_reservation_date_key,
    fr_dr_cluster_type,
    destination_cluster,
    cluster_city,
    bu,
    super_category,
    vertical,
    fsn,
    is_first_party_seller
"""

# --- 4. EXECUTION ---
if __name__ == "__main__":
    logging.info(f"Report Window: {start_date} to {d_minus_1}")
    full_df = get_full_dataframe(sql_query)
    
    if full_df is not None:
        print("\n--- RESULTS ---")
        print(full_df.head())
        # To save to a file automatically:
        # full_df.to_excel(f"Sales_Report_{d_minus_1}.xlsx", index=False)
    else:
        print("Report generation failed.")


full_df

final = full_df[full_df['cluster_code'].notna()]
final



cread = {
  "type": "service_account",
  "project_id": "planning-sh-perfomance",
  "private_key_id": "abf2c13b2d66d213a282a6f884af9423fde34e33",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCaHpb+hkB89Kjp\nliNqBilJVJI74DE+5q3D6ltkQMyIeqPCXyhZp4tuN+K0BLD25HuinLPV9kDyWOwl\nzWB4v96NQ5OQsAVL1mdEIP9zaL9qlk3XHN/xo5jo+MAuO6it0590dYE3aBDB5JYX\nPECs0ciBxLNeY2OhNm7hbwBus9uqkaIAWkq1LV13wZ1JLNcU+Mw2U04P8OTcfHGN\nBo/7yD84Y4xgtU2JsLIVyVfIdtyQ6UjkwyEBaN3g0fMI5KW1dqYF6FHOF02Mj17P\n6SZovuOumIpiHY9LXAHX7RKxUZ4Z7UlpTXfN+OvT0S/7WuJ3ygXZqmSaO1f5/QFJ\nJ6Gm+UkpAgMBAAECggEADsT8smKaPn9RyqTVOv1hKoJcyzkmbwE7SMmmPXrKPxAF\nroRhAQqB3aukYZ0LenWV9ZTv1QLW4YKtCsheROQaR05hD14W+9gNqGVDJcO1iFjk\nF8fQHD+R0U25WRTlitb7cnUqT219IZ+u1IY1KnqmLO11RQfgTdxCDv+Muo8o73UE\nTMc4SguO1RovR+mv0J4S5ORDcUeHyRT1ppMmpDF7FfnQi4rFdjM5nQnDu+QgAMJ6\nzAZiCmz1INnZYvWhWY7aTwGZiJXR44l1V8RNKKwpeEVP4es0CpuG7U0mGXHJWjVl\n8Uxhux/xlhljB/pG5QSfhOTpqDqJAYBaqqCzg9uEgwKBgQDLmIxc7m8pb6/tFWsW\nUkbS8NBXnXS60g5QrH+EFqOoWjupLL/T8ObbWncNQtBujwtkYtrgSqH/qnfH2i1V\nNMxvkGj1S5mG94WNPj07eCD5cka+xOFKBEroKxCwVwVbc/+hSY9nqtR1q55ztPqR\nSJxSkoeB07t1TrzmhK1qjgATwwKBgQDByepgluqD+lT4HwLB81kdNC6DMYiIVkGW\nY86DIKtlLoptNJkhboguJ01zJ48T80uegAZfX8W5eVgPT0iTCZK4Kp+FLoeJ/yVL\n53z2yDWkb1x9KPs3sQW/NodCOqwEM862gTBgVZ4vmQvdTAJTx+4SG5swqf/tQ+1A\nNRCeKNk8owKBgQC1QnQ8zH6lSm07S2VSsx+g74rdZi6loRvjkR/aDnnYCbWyEUgb\nvg8aXXk+kiyVMb83uZcaNvfxcehAQqs3f9E+xjfbo1nlQnthW3cSegoJa0c20nus\n9RNnjefGx0Lav/RnuOD2r62FsaxZYVfvftDF5vTDjikH8HjXVmo6QoApFwKBgQCg\nMeouLpNuxmG3OutsqV3hZmGM0kWrbqKJT7hHbZpB6ldEL1bm7BhnDtZXezwrodPB\nEtQxw1oQGN9SyJeV9TP0SkfUMMKasPCD7ri+yIKPi/9I97e0MgIuL0Vg2l+Ymbsn\n42O6PwylDD9ikJLb70o+bdO8Rsvpt11I1pUGKX5rQQKBgGqNudqS2HWsOEXUNEQl\nKXfB9AoqDKeFUSLNjqJyOZ0yTKTxlXatPIiuTq1Z32Xv16pjAWcDbmj4MQg9YpyH\nMRCF8opOYK8bw49FQQvwNxRKaLefpZOpEipLOrCYMtxFUXhy9RWaiosaaZ/WVNW4\nlBxuebXqNZDueOx6ZT1zVfKH\n-----END PRIVATE KEY-----\n",
  "client_email": "planning-sh-perfomance@planning-sh-perfomance.iam.gserviceaccount.com",
  "client_id": "101930403964338635316",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/planning-sh-perfomance%40planning-sh-perfomance.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}


scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

credentials = service_account.Credentials.from_service_account_info(cread, scopes=scopes)

service = build('sheets', 'v4', credentials=credentials)

gc = gspread.authorize(credentials)
SPREADSHEET_ID1 = '1pFAbQpZI8KJqoYT4w7PQXuFxpr_Mun4PVC-euRGZXi8'

sh1 = gc.open_by_key(SPREADSHEET_ID1)

worksheet = sh1.worksheet("Mapping")
mapping_df = pd.DataFrame(worksheet.get_all_records())

final_df = pd.merge(final,mapping_df[['fsn','Group']],on='fsn',how='left')
final_df
wks1 = sh1.worksheet('Last 10 days')

wks1.clear()
set_with_dataframe(wks1, final_df, include_index=False, include_column_header=True)

