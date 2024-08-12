import os
import psycopg2 as pg
import pandas as pd
import logging

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

# Update materialized view
def update__matview(db_params, table):
    """Create Materialized View."""
    user = db_params['user']
    with open('automations/queries/all_donations.sql', 'r') as file:
        all_donations_query = file.read()
    command = f"""
        -- Create a new schema for experimental views
        CREATE SCHEMA IF NOT EXISTS experimental_views;

        -- Create the materialized view in the new schema
        DROP MATERIALIZED VIEW IF EXISTS experimental_views.{table}_local;
        CREATE MATERIALIZED VIEW experimental_views.{table}_local AS
        (
        SELECT
            *
        FROM
            public.{table}
        );

        -- Grant necessary permissions (adjust as needed)
        GRANT USAGE ON SCHEMA experimental_views TO {user};
        GRANT SELECT ON experimental_views.{table}_local TO {user};"""
    execute_command(command, db_params)

# Main execution logic
def main():
    
        for table in ['applications', 'rounds', 'donations']:
            try:
                update__matview(DB_PARAMS, table)
            except Exception as e:
                logger.error(f"Failed to update materialized view {table}_local: {e}")

if __name__ == "__main__":
    main()
