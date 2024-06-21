import os
import psycopg2 as pg
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_PARAMS = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'dbname': os.getenv('DB_DBNAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

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

# Update all donations materialized view
def update_all_donations_matview(db_params):
    """Create Materialized View for all donations."""
    print(os.getcwd())
    with open('queries/all_donations.sql', 'r') as file:
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
