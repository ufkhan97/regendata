import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from sqlalchemy import create_engine
import json
import os

# Load database credentials from environment variables
host = os.environ['DB_HOST']
port = os.environ['DB_PORT']
dbname = 'Grants'
user = os.environ['DB_USER']
password = os.environ['DB_PASSWORD']

# Load Google credentials from environment variables
google_creds_json = json.loads(os.environ['GOOGLE_CREDENTIALS'])  # Assumes JSON string
creds = ServiceAccountCredentials.from_json_keyfile_dict(google_creds_json, ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/drive'])
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

