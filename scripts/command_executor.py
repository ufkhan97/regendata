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

def execute_and_fetch(command):
    connection = None
    try:
        connection = psycopg2.connect(**DB_PARAMS)
        connection.autocommit = True
        cursor = connection.cursor()
        
        for line in command.strip().split('\n'):
            line = line.strip()
            if line and not line.startswith('--'):
                cursor.execute(line)
                if cursor.description:  # Only fetch if there are results
                    result = cursor.fetchone()
                    if result:
                        print(f"{line}: {result[0]}")
        logger.info("Command executed successfully.")
    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        raise
    finally:
        if connection:
            connection.close()

def main():
    command = """
        SHOW tcp_keepalives_idle;
        SHOW tcp_keepalives_interval;
        SHOW tcp_keepalives_count;
        SELECT pg_reload_conf();
    """

    try:
        execute_and_fetch(command)
        logger.info("Successfully executed the command")
    except Exception as e:
        logger.error(f"Failed to complete operation: {e}", exc_info=True)

if __name__ == "__main__":
    main()