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

def create_static_table(table):
    static_table_name = f'static_{table}_chain_data_75'
    
    # Create the table
    create_table_command = f"""
    BEGIN;
        DROP TABLE IF EXISTS experimental_views.{static_table_name};
        CREATE TABLE experimental_views.{static_table_name} AS 
        SELECT * FROM experimental_views.{table}_local;
    COMMIT;
    """
    execute_command(create_table_command)
    logger.info(f"Created static table {static_table_name}")
    
    # Grant permissions to users
    for user in USERS:
        if user:  # Skip empty strings
            grant_command = f"""
            BEGIN;
                GRANT USAGE ON SCHEMA experimental_views TO {user};
                GRANT SELECT ON experimental_views.{static_table_name} TO {user};
            COMMIT;
            """
            execute_command(grant_command)
            logger.info(f"Granted permissions to user {user} on {static_table_name}")

def main():
    tables = ['applications', 'rounds', 'donations']
    for table in tables:
        try:
            logger.info(f"Starting creation of static table for {table}")
            start_time = time.time()
            create_static_table(table)
            end_time = time.time()
            logger.info(f"Successfully created static table for {table} in {end_time - start_time} seconds")
        except Exception as e:
            logger.error(f"Failed to create static table for {table}: {e}", exc_info=True)

if __name__ == "__main__":
    main()