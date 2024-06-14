import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from sqlalchemy import create_engine
import json

# Load database credentials
with open('grantsdb_credentials.json') as f:
    creds = json.load(f)
host = creds['DB_HOST']
port = creds['DB_PORT']
dbname = creds['DB_NAME']
user = creds['DB_USER']
password = creds['DB_PASSWORD']

# Setup Google Sheets API & Authorize
scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('regendata-ingestor-ac8b2979b4c0.json', scope)
client = gspread.authorize(creds)

# Open the spreadsheet and get the first sheet
spreadsheet_id = '1Jx3RgIKkuhhzVFvUSjRgOEJcpRfpu-7WlPd8wLUGdxE'
sheet = client.open_by_key(spreadsheet_id).sheet1
data = sheet.get_all_records()

# Convert to DataFrame
df = pd.DataFrame(data)
df['timestamp'] = pd.to_datetime(df['timestamp'])
df['amount_in_usd'] = df['amount_in_usd'].replace('[\$,]', '', regex=True).astype(float)


## UPLOAD df TO POSTGRES
table = 'AlloRoundsOutsideIndexer'
engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')
try:
    df.to_sql(table, engine, if_exists='replace', index=False)
    print(f"Data successfully written to database table {table}.")
except Exception as e:
    print("Failed to write data to database:", e)

