from dune_client.client import DuneClient
import pandas as pd
import logging
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
import hashlib
import psycopg2



# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
try:
    if load_dotenv():
        logger.info("Loaded .env file")
    else:
        logger.info("No .env file found or loaded")
except ImportError:
    logger.info("dotenv not installed, skipping .env file loading")


# Database configuration
DB_PARAMS = {
    'host': os.environ.get('DB_HOST'),
    'port': os.environ.get('DB_PORT'),
    'dbname': 'Grants',
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD')
}
# Get Dune API key
DUNE_API_KEY = os.getenv('DUNE_API_KEY')

def get_connection(logger):
    """Establish database connection with proper settings."""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        with conn.cursor() as cursor:
            # Set keepalive parameters
            cursor.execute("SET tcp_keepalives_idle = 180;")  # 3 minutes
            cursor.execute("SET tcp_keepalives_interval = 60;")  # 60 seconds
        return conn
    except psycopg2.Error as e:
        logger.error(f"Failed to connect to the database: {e}")
        raise

def execute_command(logger, connection, command: str, params: tuple = None) -> None:
    """Execute a database command with proper error handling."""
    logger.info(f"Executing command: {command[:100]}...")
    try:
        with connection.cursor() as cursor:
            if params:
                cursor.execute(command, params)
            else:
                cursor.execute(command)
        connection.commit()
        logger.info("Command executed successfully.")
    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        connection.rollback()
        raise

def refresh_dune_table(dune_api_key, logger):
    try:
        # Initialize Dune client and get query results
        logger.info("Initializing Dune client")
        dune = DuneClient(dune_api_key)
        logger.info("Fetching latest results from query 4118421")
        query_result = dune.get_latest_result(4118421)
        logger.info("Successfully retrieved query results")
        query_result_df = pd.DataFrame(query_result.result.rows)
        
        # Sort dataframe and add row number column
        query_result_df = query_result_df.sort_values(by=['tx_timestamp','role', 'address', 'gmv'])
        query_result_df['row_number'] = range(1, len(query_result_df) + 1)
        
        # Create hash_id by concatenating and hashing relevant columns
        query_result_df['event_signature'] = query_result_df.apply(
            lambda row: hashlib.sha256(
                f"{row['tx_timestamp']}{row['tx_hash']}{row['address']}{row['gmv']}{row['role']}{row['row_number']}".encode()
            ).hexdigest(),
            axis=1
        )

        # validation
        if len(query_result_df) == 0:
            raise ValueError("Empty result set from Dune")

        # Convert dataframe to SQL values
        values = []
        for _, row in query_result_df.iterrows():
            value_list = [
                f"'{str(v)}'" if isinstance(v, (str, pd.Timestamp)) else str(v) if v is not None else 'NULL'
                for v in row.values
            ]
            values.append(f"({', '.join(value_list)})")

        create_command = f"""
        DROP MATERIALIZED VIEW IF EXISTS public.allov2_distribution_events_for_leaderboard CASCADE;
        CREATE MATERIALIZED VIEW public.allov2_distribution_events_for_leaderboard AS
        SELECT * FROM (
            VALUES {','.join(values)}
        ) AS t({', '.join(f'"{col}"' for col in query_result_df.columns)});
        """

        # Get database connection and execute command
        connection = get_connection(logger)
        execute_command(logger, connection, create_command)
        logger.info(f"Successfully created materialized view with {len(query_result_df)} rows")

    except Exception as e:
        logger.error(f"Failed to refresh Dune table: {e}")
        raise
    finally:
        if 'connection' in locals():
            connection.close()

def main():
    refresh_dune_table(DUNE_API_KEY, logger)

if __name__ == "__main__":
    main()
