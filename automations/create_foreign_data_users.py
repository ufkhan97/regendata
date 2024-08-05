# Establish which Grants DB Users have access to Foreign Data
# Such as the Indexer and MACI databases

import pandas as pd
import psycopg2 as pg
from psycopg2 import sql
import logging
import os

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def run_query(query, db_params):
    """Run a query and return the results as a DataFrame."""
    try:
        with pg.connect(**db_params) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                col_names = [desc[0] for desc in cur.description]
                results = pd.DataFrame(cur.fetchall(), columns=col_names)
                return results
    except pg.Error as e:
        logger.error(f"ERROR: Could not execute the query. {e}")
        return None

def execute_command(command, db_params):
    """Execute a SQL command that doesn't return results."""
    try:
        with pg.connect(**db_params) as conn:
            with conn.cursor() as cur:
                cur.execute(command)
                conn.commit()
                logger.info("Command executed successfully.")
    except pg.Error as e:
        logger.error(f"ERROR: Could not execute the command. {e}")

def create_server(server_name, host, dbname, port, DB_PARAMS):
    """Create a new server connection"""
    create_server_query = f"""
    CREATE SERVER {server_name}
    FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (host '{host}', dbname '{dbname}', port '{port}');
    """
    execute_command(create_server_query, DB_PARAMS)

def create_user_mapping(server_name, db_user, fdw_user,  fdw_password, DB_PARAMS):
    """Create a user mapping for the server"""
    create_user_mapping_query = f"""
    CREATE USER MAPPING FOR {db_user}
    SERVER {server_name}
    OPTIONS (user '{fdw_user}', password '{fdw_password}');
    """
    execute_command(create_user_mapping_query, DB_PARAMS)

def main():
    # Try to load .env file if it exists (for local development)
    try:
        from dotenv import load_dotenv
        if load_dotenv():
            logger.info("Loaded .env file")
        else:
            logger.info("No .env file found or loaded")
    except ImportError:
        logger.info("dotenv not installed, skipping .env file loading")


    # Database connection details
    INDEXER_DB_PARAMS = {
        'host': os.getenv('INDEXER_DB_HOST'),
        'port': os.getenv('INDEXER_DB_PORT'),
        'dbname': os.getenv('INDEXER_DB_NAME'),
        'user': os.getenv('INDEXER_DB_USER'),
        'password': os.getenv('INDEXER_DB_PASSWORD')
    }

    DB_PARAMS = {
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT'),
        'dbname': 'Grants',
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD')
    }

    MACI_DB_PARAMS = {
        'host': os.getenv('MACI_DB_HOST'),
        'port': os.getenv('MACI_DB_PORT'),
        'dbname': os.getenv('MACI_DB_NAME'),
        'user': os.getenv('MACI_DB_USER'),
        'password': os.getenv('MACI_DB_PASSWORD')
    }

    USERS = os.getenv('DB_FDW_USERS').strip('[]').replace("'", "").split(', ')
    
    # Create server connections
    create_server('indexer', INDEXER_DB_PARAMS['host'], INDEXER_DB_PARAMS['dbname'], INDEXER_DB_PARAMS['port'], DB_PARAMS)
    create_server('maci', MACI_DB_PARAMS['host'], MACI_DB_PARAMS['dbname'], MACI_DB_PARAMS['port'], DB_PARAMS)

    # Create user mappings
    for user in USERS:
        create_user_mapping('indexer', user, INDEXER_DB_PARAMS['user'], INDEXER_DB_PARAMS['password'], DB_PARAMS)
        create_user_mapping('maci', user, MACI_DB_PARAMS['user'], MACI_DB_PARAMS['password'], DB_PARAMS)

if __name__ == "__main__":
    main()

