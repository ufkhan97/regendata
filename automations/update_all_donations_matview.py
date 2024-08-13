import os
import psycopg2 as pg
import pandas as pd
import logging
import threading
import time  # Import the time module

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Try to load .env file if it exists (for local development)
try:
    from dotenv import load_dotenv
    if load_dotenv():
        logger.info("Loaded .env file")
    else:
        logger.info("No .env file found or loaded")
except ImportError:
    logger.info("dotenv not installed, skipping .env file loading")


DB_PARAMS = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'dbname': 'Grants',
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

def keep_connection_alive(cursor, interval=60):
    """Periodically execute a simple query to keep the connection alive."""
    try:
        while True:
            cursor.execute("SELECT 1;")
            logger.info("Keep-alive query executed.")
            time.sleep(interval)
    except Exception as e:
        logger.error(f"Keep-alive mechanism failed: {e}")

def execute_command(command, db_params):
    """Execute a SQL command that doesn't return results."""
    try:
        with pg.connect(**db_params) as conn:
            with conn.cursor() as cur:
                # Start a background thread to keep the connection alive
                keep_alive_thread = threading.Thread(target=keep_connection_alive, args=(cur,))
                keep_alive_thread.start()

                cur.execute("SET statement_timeout = 0;")
                cur.execute(command)
                conn.commit()

                logger.info("Command executed successfully.")
                
                # Signal the keep-alive thread to stop (using a flag or timeout)
                keep_alive_thread.join(timeout=1)
    except pg.Error as e:
        logger.error(f"ERROR: Could not execute the command. {e}")

# Update all donations materialized view
def update_all_donations_matview(db_params):
    """Create Materialized View for all donations."""
    with open('automations/queries/all_donations.sql', 'r') as file:
        all_donations_query = file.read()
    create_command = f"""
                        DROP MATERIALIZED VIEW IF EXISTS all_donations;
                        CREATE MATERIALIZED VIEW all_donations AS
                        ({all_donations_query})
                        """
    execute_command(create_command, db_params)

# Main execution logic
def main():
    try:
        update_all_donations_matview(DB_PARAMS)
    except Exception as e:
        logger.error(f"Failed to update all donations materialized view: {e}")

if __name__ == "__main__":
    main()
