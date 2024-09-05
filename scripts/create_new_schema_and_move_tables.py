import os
import psycopg2
import logging
import time
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

# Load FDW users from environment variable
USERS = os.getenv('DB_FDW_USERS', '').strip('[]').replace("'", "").split(', ')

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

def create_schema(schema_name):
    command = f"""
    BEGIN;
        -- Create the schema if it doesn't exist
        CREATE SCHEMA IF NOT EXISTS {schema_name};
    COMMIT;
    """
    execute_command(command)
    logger.info(f"Created new schema structure: {schema_name}")

def move_table(old_schema, old_table, new_table, new_schema):
    command = f"""
    BEGIN;
        -- Move the table to the new schema
        ALTER TABLE IF EXISTS {old_schema}.{old_table} 
        SET SCHEMA {new_schema};
        
        -- Rename the table
        ALTER TABLE IF EXISTS {new_schema}.{old_table}
        RENAME TO {new_table};
    COMMIT;
    """
    execute_command(command)
    logger.info(f"Moved and renamed table to {new_schema}.{new_table}")
    
    # Grant permissions to users
    for user in USERS:
        if user:  # Skip empty strings
            grant_command = f"""
            BEGIN;
                GRANT USAGE ON SCHEMA {new_schema.split('.')[0]} TO {user};
                GRANT USAGE ON SCHEMA {new_schema} TO {user};
                GRANT SELECT ON {new_schema}.{new_table} TO {user};
            COMMIT;
            """
            execute_command(grant_command)
            logger.info(f"Granted permissions to user {user} on {new_schema}.{new_table}")

def main():
    old_schema = 'experimental_views'
    new_schema = 'static_indexer_chain_data_75'
    old_tables = ['static_donations_chain_data_75', 'static_applications_chain_data_75', 'static_rounds_chain_data_75']
    new_tables = ['donations', 'applications', 'rounds']
    try:
        create_schema(new_schema)
        for old_table, new_table in zip(old_tables, new_tables):
            logger.info(f"Starting move of table {old_table}")
            start_time = time.time()
            move_table(old_schema, old_table, new_table, new_schema)
            end_time = time.time()
            logger.info(f"Successfully moved table {old_table} to {new_table} in {end_time - start_time} seconds")
    except Exception as e:
        logger.error(f"Failed to complete operation: {e}", exc_info=True)

if __name__ == "__main__":
    main()