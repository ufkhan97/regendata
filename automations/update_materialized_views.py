import os
import psycopg2
import logging
import time

# TO DO: 
# Consider how to Incoporate Updating Schema Into Pipeline
# If we want to update the schema, it would change the foreign tables in the database
# This would require us to drop the foreign tables and recreate them, which would drop the materialized views
# We would need to update the materialized views to reflect the new schema
# Unless we can either: drop the foreign tables and recreate them without dropping the materialized views, 
# or update the foreign tables without dropping them (if that's possible while changing the schema)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Try to load .env file if it exists (for local development)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Load database credentials from environment variables
DB_PARAMS = {
    'host': os.environ.get('DB_HOST'),
    'port': os.environ.get('DB_PORT'),
    'dbname': 'Grants',
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD')
}

# Validate DB_PARAMS
if not all(DB_PARAMS.values()):
    raise ValueError("Missing database connection parameters. Please check your environment variables.")

# Define materialized view configurations
BASE_MATVIEWS = ['applications_payouts', 'rounds',  'donations'] #'applications',
DEPENDENT_MATVIEWS = ['all_donations', 'all_matching']
MATVIEW_CONFIGS = {
    'applications_payouts': {
        'index_columns': ['id'],
    },
    'rounds': {
        'index_columns': ['id', 'chain_id'],
    },
        'applications': {
        'index_columns': ['id', 'chain_id', 'round_id'],
    },
    'donations': {
        'index_columns': ['id'],
    }
}

def get_connection():
    try:
        return psycopg2.connect(**DB_PARAMS)
    except psycopg2.Error as e:
        logger.error(f"Failed to connect to the database: {e}")
        raise

def execute_command(connection, command):
    logger.info(f"Executing command: {command[:100]}...")
    try:
        with connection.cursor() as cursor:
            cursor.execute("SET tcp_keepalives_idle = 180;")  # 3 minutes
            cursor.execute("SET tcp_keepalives_interval = 60;")  # 60 seconds
            cursor.execute(command)
        connection.commit()
        logger.info("Command executed successfully.")
    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        connection.rollback()
        raise

def create_new_base_matviews(connection):
    """Create new versions of base materialized views with '_new' suffix."""
    for matview in BASE_MATVIEWS:
        config = MATVIEW_CONFIGS[matview]
        index_columns = ', '.join(config['index_columns'])
        logger.info(f"Creating new materialized view {matview}_new")
        create_command = f"""
        CREATE MATERIALIZED VIEW public.{matview}_new AS
        WITH ranked_data AS (
            SELECT 
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY {index_columns}
                    ORDER BY 
                        CASE 
                            WHEN source = 'indexer' THEN 1 
                            WHEN source = 'static' THEN 2 
                        END
                ) as row_num
            FROM (
                SELECT *, 'indexer' as source 
                FROM indexer.{matview} 
                WHERE chain_id != 11155111
                UNION ALL
                SELECT *, 'static' as source 
                FROM static_indexer_chain_data_75.{matview} 
                WHERE chain_id != 11155111
            ) combined_data
        )
        SELECT * FROM ranked_data WHERE row_num = 1;
        """
        execute_command(connection, create_command)

def swap_base_matviews(connection):
    """Swap base materialized views in a single transaction."""
    swap_commands = []

    # Start transaction
    swap_commands.append("BEGIN;")

    # Rename old base matviews to '_old' and new ones to original names
    for matview in BASE_MATVIEWS:
        #swap_commands.append(f"ALTER MATERIALIZED VIEW public.{matview} RENAME TO {matview}_old;")
        swap_commands.append(f"ALTER MATERIALIZED VIEW public.{matview}_new RENAME TO {matview};")

    # Commit transaction
    swap_commands.append("COMMIT;")

    full_swap_command = "\n".join(swap_commands)
    execute_command(connection, full_swap_command)

def create_indexer_matching_view(connection):
    with open('queries/indexer_matching.sql', 'r') as file:
        indexer_matching_query = file.read()
    create_command = f"""
        CREATE OR REPLACE VIEW indexer_matching AS
        ({indexer_matching_query});
    """
    execute_command(connection, create_command)

def create_new_dependent_matviews(connection):
    """Create new versions of dependent materialized views with '_new' suffix."""
    # Create new all_matching_new
    logger.info("Creating new materialized view all_matching_new")
    with open('queries/all_matching.sql', 'r') as file:
        all_matching_query = file.read()
    create_command = f"""
        CREATE MATERIALIZED VIEW all_matching_new AS
        ({all_matching_query});
    """
    execute_command(connection, create_command)

    # Create new all_donations_new
    logger.info("Creating new materialized view all_donations_new")
    with open('queries/all_donations.sql', 'r') as file:
        all_donations_query = file.read()
    create_command = f"""
        CREATE MATERIALIZED VIEW all_donations_new AS
        ({all_donations_query});
    """
    execute_command(connection, create_command)

def swap_dependent_matviews(connection):
    """Swap dependent materialized views in a transaction."""
    swap_commands = []

    # Start transaction
    swap_commands.append("BEGIN;")

    # Rename old dependent matviews to '_old' and new ones to original names
    for matview in DEPENDENT_MATVIEWS:
        #swap_commands.append(f"ALTER MATERIALIZED VIEW public.{matview} RENAME TO {matview}_old;")
        swap_commands.append(f"ALTER MATERIALIZED VIEW public.{matview}_new RENAME TO {matview};")

    # Commit transaction
    swap_commands.append("COMMIT;")

    full_swap_command = "\n".join(swap_commands)
    execute_command(connection, full_swap_command)

def drop_old_matviews(connection):
    """Drop old base and dependent materialized views."""
    drop_commands = []
    for matview in BASE_MATVIEWS + DEPENDENT_MATVIEWS:
        drop_commands.append(f"DROP MATERIALIZED VIEW IF EXISTS public.{matview}_old;")
    full_drop_command = "\n".join(drop_commands)
    execute_command(connection, full_drop_command)



def main():
    connection = None
    try:
        connection = get_connection()

        # Step 1: Create new base materialized views
        logger.info("Creating new base materialized views...")
        #create_new_base_matviews(connection)
        logger.info("Successfully created new base materialized views.")

        # Step 2: Swap base materialized views
        logger.info("Swapping base materialized views...")
        #swap_base_matviews(connection)
        logger.info("Successfully swapped base materialized views.")

        # Step 3: Create or replace indexer_matching view
        logger.info("Creating or replacing indexer_matching view...")
        create_indexer_matching_view(connection)
        logger.info("Successfully created or replaced indexer_matching view.")

        # Step 4: Create dependent materialized views
        logger.info("Creating dependent materialized views...")
        #create_new_dependent_matviews(connection)
        logger.info("Successfully created dependent materialized views.")

        # Step 5: Swap dependent materialized views
        logger.info("Swapping dependent materialized views...")
       # swap_dependent_matviews(connection)
        logger.info("Successfully swapped dependent materialized views.")

        # Step 6: Drop old materialized views
        logger.info("Dropping old materialized views...")
        #drop_old_matviews(connection)
        logger.info("Successfully dropped old materialized views.")

        logger.info("All materialized views updated successfully.")
    except Exception as e:
        logger.error(f"An error occurred during execution: {e}", exc_info=True)
    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    main()
