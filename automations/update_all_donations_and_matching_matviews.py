import os
import psycopg2 as pg
import pandas as pd
import logging


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
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
                cur.execute("SET tcp_keepalives_idle = 180;")  # 3 minutes
                cur.execute("SET tcp_keepalives_interval = 60;")  # 60 seconds
                cur.execute(command)
                conn.commit()

                logger.info("Command executed successfully.")
                
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

def update_all_matching_matview(db_params):
    """Create Materialized View for all matching."""
    with open('automations/queries/all_matching.sql', 'r') as file:
        all_matching_query = file.read()
    create_command = f"""
                        DROP MATERIALIZED VIEW IF EXISTS all_matching;
                        CREATE MATERIALIZED VIEW all_matching AS
                        ({all_matching_query})
                        """
    execute_command(create_command, db_params)

# Main execution logic
def main():
    try:
        logger.info("Starting update of materialized views...")
        
        logger.info("Updating all_matching materialized view...")
        update_all_matching_matview(DB_PARAMS)
        logger.info("Successfully updated all_matching materialized view.")
        
        logger.info("Updating all_donations materialized view...")
        update_all_donations_matview(DB_PARAMS)
        logger.info("Successfully updated all_donations materialized view.")
        
        logger.info("All materialized views updated successfully.")
    except pg.Error as e:
        logger.error(f"Database error occurred while updating materialized views: {e}")
    except IOError as e:
        logger.error(f"IO error occurred while reading SQL files: {e}")
    except Exception as e:
        logger.error(f"Unexpected error occurred while updating materialized views: {e}")

if __name__ == "__main__":
    main()


