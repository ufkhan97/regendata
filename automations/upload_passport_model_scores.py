import pandas as pd
from sqlalchemy import create_engine, text
import json
import os
import logging
import psycopg2

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

try:
    from dotenv import load_dotenv
    if load_dotenv():
        logger.info("Loaded .env file")
    else:
        logger.info("No .env file found or loaded")
except ImportError:
    logger.info("dotenv not installed, skipping .env file loading")

# Load database credentials from environment variables
DB_PARAMS = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'dbname': 'Grants',
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

def execute_command(command):
    logger.info(f"Executing command: {command[:50]}...")  # Log first 50 characters
    connection = None
    try:
        connection = psycopg2.connect(**DB_PARAMS)
        cursor = connection.cursor()
        cursor.execute("SET tcp_keepalives_idle = 180;")  # 3 minutes
        cursor.execute("SET tcp_keepalives_interval = 60;")  # 60 seconds
        cursor.execute(command)
        connection.commit()
        logger.info("Command executed successfully.")
    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        if connection:
            connection.rollback()
        raise
    finally:
        if connection:
            connection.close()

# Define a function to unnest the 'value' column
def clean_model_scores(df):
    # Parse and normalize the JSON data in the specified column
    column = 'value'
    unnested_df = pd.json_normalize(df[column].apply(json.loads))
    
    # Replace '.' with '_' in column names and convert to lowercase
    unnested_df.columns = unnested_df.columns.str.replace('.', '_').str.lower()
    
    # Concatenate the unnested dataframe with the original dataframe
    df = pd.concat([df.drop(columns=[column]), unnested_df], axis=1)
    
    # Rename columns 'key_0' to 'model' and 'key_1' to 'address'
    df.rename(columns={'key_0': 'model', 'key_1': 'address'}, inplace=True)
    
    # Drop specified columns from the dataframe
    columns_to_drop = [
        'data_meta_version', 'data_meta_training_date', 'data_gas_spent',
        'data_n_days_active', 'data_n_transactions', 'data_has_safe_nft'
    ]
    df.drop(columns=columns_to_drop, inplace=True)
    return df

def upload_to_postgres(df, table_name):
    temp_table = f"{table_name}_temp"
    
    # Create SQLAlchemy engine
    engine = create_engine(f'postgresql://{DB_PARAMS["user"]}:{DB_PARAMS["password"]}@{DB_PARAMS["host"]}:{DB_PARAMS["port"]}/{DB_PARAMS["dbname"]}')
    
    try:
        with engine.begin() as conn:
            # Use to_sql to create temporary table and insert data into the temporary table
            df.to_sql(temp_table, conn, if_exists='replace', index=False, method='multi', chunksize=1000)
            logger.info(f"Data successfully written to temporary table {temp_table}.")
            
            # Drop the main table if it exists
            drop_main_table_query = text(f"DROP TABLE IF EXISTS {table_name};")
            conn.execute(drop_main_table_query)
            logger.info(f"Main table {table_name} dropped.")
            
            # Rename the temporary table to the main table
            rename_temp_table_query = text(f"ALTER TABLE {temp_table} RENAME TO {table_name};")
            conn.execute(rename_temp_table_query)
            logger.info(f"Succesfully renamed temporary table {temp_table} to {table_name}.")
            
    except Exception as e:
        logger.error(f"Failed to write data to database: {e}")
        raise


# Usage
def main():
    url = 'https://nyc3.digitaloceanspaces.com/regendata/passport/model_scores.parquet'
    logger.info(f"Reading parquet file from URL: {url}")
    try:
        df = pd.read_parquet(url, engine='fastparquet')
        logger.info("Successfully read parquet file from URL.")
    except Exception as e:
        logger.error(f"Failed to read parquet file from URL: {e}")
        df = None
    
    if df is not None:
        df = clean_model_scores(df)
        upload_to_postgres(df, 'passport_model_scores')

if __name__ == "__main__":
    main()