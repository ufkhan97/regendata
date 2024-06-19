import pandas as pd
import numpy as np
from datetime import datetime, timezone
import psycopg2 as pg
import networkx as nx
import itertools
from collections import defaultdict
from sqlalchemy import create_engine
import os


# Load database credentials from environment variables
host = os.environ['DB_HOST']
port = os.environ['DB_PORT']
dbname = os.environ['DB_NAME']
user = os.environ['DB_USER']
password = os.environ['DB_PASSWORD']

def run_query(query):
    """Run query and return results"""
    try:
        conn = pg.connect(host=host, port=port, dbname=dbname, user=user, password=password)
        cur = conn.cursor()
        cur.execute(query)
        col_names = [desc[0] for desc in cur.description]
        results = pd.DataFrame(cur.fetchall(), columns=col_names)
    except pg.Error as e:
        print(f"ERROR: Could not execute the query. {e}")
    finally:
        conn.close()
    return results

def clean_github(url_or_name):
    if pd.isna(url_or_name):
        return np.nan
    # Remove 'https://github.com/' if it's present
    url_or_name = url_or_name.replace('https://github.com/', '').replace('http://github.com/', '')
    if '/' in url_or_name:
        # If it's a URL or a 'username/repo' string, split by '/' and take the first part
        return url_or_name.split('/')[0].lower()
    else:
        # If it's a username, return it as is
        return url_or_name.lower()

cgrants_query = '''
    SELECT
        cg.grantid AS project_id,
        cg.name AS title,
        cg.websiteurl AS website,
        LOWER(cg.payoutaddress) AS payout_address,
        cg.twitter AS project_twitter,
        cg.github AS project_github,
        cg.createdon AS created_at,
        ad.amount_donated, 
        ad.last_donation,
        'cGrants' as source
    FROM
        public."cgrantsGrants" cg
    LEFT JOIN (
        SELECT
            grant_id,
            SUM("amountUSD") AS amount_donated,
            max("created_on") AS last_donation
        FROM
            public."cgrantsContributions"
        GROUP BY
            grant_id
    ) ad ON cg.grantid = ad.grant_id;
    '''

indexer_query = '''
    SELECT
        project_id,
        metadata #>> '{application, project, projectGithub}' AS project_github,
        metadata #>> '{application, project, projectTwitter}' AS project_twitter,
        metadata #>> '{application, project, title}' AS title,
        metadata #>> '{application, project, website}' AS website,
        metadata #>> '{application, recipient}' AS payout_address,
        TO_TIMESTAMP(CAST((metadata #>> '{application, project, createdAt}') AS bigint)/1000) AS "created_at",
        total_amount_donated_in_usd AS "amount_donated",
        'indexer' AS source
    FROM
        applications
    WHERE 
        chain_id != 11155111 ;
    '''
# Set the minimum number of shared attributes required to draw an edge
min_shared_attributes = 2  # Change this value as needed

cgrants_data = run_query(cgrants_query)
indexer_data = run_query(indexer_query)
cgrants_data['project_id'] = cgrants_data['project_id'].astype(str)
data = pd.concat([cgrants_data, indexer_data], ignore_index=True)

# Pre-cleaning
eth_address_pattern = r'^0x[a-fA-F0-9]{40}$'
data['payout_address'] = data['payout_address'].astype(str)
data = data[data['payout_address'].str.match(eth_address_pattern)]
data['title'] = data['title'].astype(str)
data['title'] = data['title'].str.lower()
data['title'] = data['title'].str.replace(r'[^\w\s]', '', regex=True)  # Remove punctuation
data['title'] = data['title'].str.replace(r'\s+', ' ', regex=True)  # Replace multiple spaces with a single space
data['title'] = data['title'].str.strip()  # Remove leading/trailing whitespace
data['project_twitter'] = data['project_twitter'].str.lower()
data['project_github'] = data['project_github'].astype(str)
data['project_github'] = data['project_github'].str.lower()
data['project_github'] = data['project_github'].apply(clean_github)
data.replace('nan', np.nan, inplace=True)

# Convert empty strings to NaN and drop them
data['title'].replace('', np.nan, inplace=True)
data.dropna(subset=['title'], inplace=True)
# Reset group_id 
data['group_id'] = 0

# Create a graph
G = nx.Graph()
# Add nodes
for i, row in data.iterrows():
    G.add_node(i)
# Create a counter for shared attributes
shared_attributes_counter = defaultdict(int)
# Count shared attributes
for attribute in ['title', 'website', 'payout_address', 'project_twitter', 'project_github']:
    attribute_data = data.dropna(subset=[attribute])
    for _, group in attribute_data.groupby(attribute):
        for i1, i2 in itertools.combinations(group.index, 2):
            pair = tuple(sorted((i1, i2)))  # Create a pair tuple
            shared_attributes_counter[pair] += 1
# Add edges for pairs with at least min_shared_attributes shared attributes
for pair, count in shared_attributes_counter.items():
    if count >= min_shared_attributes:
        G.add_edge(*pair)
# Find connected components and assign group IDs
group_id = 0
for component in nx.connected_components(G):
    data.loc[list(component), 'group_id'] = group_id
    group_id += 1

data['group_id'] = data['group_id'].astype(str)
project_lookup = data[['group_id', 'project_id', 'source']]
project_lookup = project_lookup.sort_values(by='group_id')

## UPLOAD project_lookup TO POSTGRES
table = 'project_lookup'
engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')
try:
    project_lookup.to_sql(table, engine, if_exists='replace', index=False)
    print(f"Data successfully written to database table {table}.")
except Exception as e:
    print("Failed to write data to database:", e)

# Create a DataFrame of the latest group information
group_info = data

# Group by 'group_id' and calculate sum of 'amount_donated' and max of 'created_at'
group_info_agg = group_info.groupby('group_id').agg({'amount_donated': 'sum', 'created_at': 'max', 'project_id': 'count'})
group_info_agg.columns = ['total_amount_donated', 'latest_created_application', 'application_count']
# Merge the aggregated data back to the group_info DataFrame
group_info = pd.merge(group_info, group_info_agg, on='group_id')
# Sort by created_at then keep the last row for each group_id
group_info.sort_values(by='created_at', inplace=True)
group_info.drop_duplicates(subset='group_id', keep='last', inplace=True)

# Sort by 'total_amount_donated' in descending order
group_info.sort_values(by='total_amount_donated', ascending=False, inplace=True)
# CLEAN UP THE DATA
group_info.drop([ 'created_at', 'amount_donated', 'last_donation'], axis=1, inplace=True)
group_info.rename(columns={'project_id': 'latest_created_project_id'}, inplace=True)

# Reorder the columns in the group_info DataFrame
group_info.rename(columns={'website': 'latest_website', 'payout_address': 'latest_payout_address', 'project_twitter': 'latest_project_twitter', 'project_github': 'latest_project_github', 'source': 'latest_source'}, inplace=True)
project_groups_summary = group_info[['group_id', 'title', 'latest_created_project_id', 'latest_website', 'latest_payout_address', 'latest_project_twitter', 'latest_project_github', 'latest_source',  'total_amount_donated', 'application_count', 'latest_created_application']]
project_groups_summary.reset_index(drop=True, inplace=True)
## UPLOAD project_groups_summary TO POSTGRES
table = 'project_groups_summary'
engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')
try:
    project_groups_summary.to_sql(table, engine, if_exists='replace', index=False)
    print(f"Data successfully written to database table {table}.")
except Exception as e:
    print("Failed to write data to database:", e)
