import os
import psycopg2
import logging
import argparse
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Try to load .env file if it exists
if load_dotenv():
    logger.info("Loaded .env file")
else:
    logger.info("No .env file found or loaded")

# Load database credentials from environment variables
DB_PARAMS = {
    'host': os.environ['DB_HOST'],
    'port': os.environ['DB_PORT'],
    'dbname': 'Grants',
    'user': os.environ['DB_USER'],
    'password': os.environ['DB_PASSWORD']
}

def execute_command(command):
    logger.info(f"Executing command: {command[:50]}...")  # Log first 50 characters
    connection = None
    try:
        connection = psycopg2.connect(**DB_PARAMS)
        cursor = connection.cursor()
        cursor.execute(command)
        connection.commit()
        logger.info("Command executed successfully.")
    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        if connection:
            connection.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def main():
    query_file_path = '../automations/queries/allo_gmv_with_ens.sql'
    try:
        with open(query_file_path, 'r') as file:
            query = file.read()
    except FileNotFoundError:
        logger.error(f"File {query_file_path} not found.")
        return

    command = f"""
    CREATE FOREIGN TABLE experimental_views.round_roles (
        chain_id integer NOT NULL,
        round_id text NOT NULL,
        address text NOT NULL,
        role text NOT NULL,
        created_at_block numeric(78,0)
    ) SERVER indexer
    OPTIONS (schema_name 'chain_data_75', table_name 'round_roles');

    DROP TABLE IF EXISTS static_indexer_chain_data_75.round_roles CASCADE;
    CREATE TABLE static_indexer_chain_data_75.round_roles AS
    SELECT * FROM experimental_views.round_roles;
    DROP FOREIGN TABLE experimental_views.round_roles;
    """

    try:
        execute_command(command)
        logger.info("Successfully executed the command")
    except Exception as e:
        logger.error(f"Failed to complete operation: {e}", exc_info=True)

if __name__ == "__main__":
    main()