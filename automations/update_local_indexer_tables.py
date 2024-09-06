import os
import psycopg2
import logging
import time

# Set up logging
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

# Load database credentials from environment variables
DB_PARAMS = {
    'host': os.environ['DB_HOST'],
    'port': os.environ['DB_PORT'],
    'dbname': 'Grants',
    'user': os.environ['DB_USER'],
    'password': os.environ['DB_PASSWORD']
}

# Define unique index columns for each table
INDEX_COLUMNS = {
    'applications': ['id', 'chain_id', 'round_id'],
    'rounds': ['id', 'chain_id'],
    'donations': ['id'],
    'applications_payouts': ['id']
}

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

def ensure_unique_index(table):
    columns = INDEX_COLUMNS.get(table)
    if not columns:
        logger.warning(f"No unique index defined for table {table}")
        return

    index_name = f"{table}_unique_idx"
    column_list = ', '.join(columns)
    
    command = f"""
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1
            FROM pg_indexes
            WHERE schemaname = 'public'
            AND tablename = '{table}'
            AND indexname = '{index_name}'
        ) THEN
            CREATE UNIQUE INDEX {index_name} ON public.{table} ({column_list});
        END IF;
    END $$;
    """
    execute_command(command)
    return 

def update_matview(table):
    user = DB_PARAMS['user']
    index_columns = ', '.join(INDEX_COLUMNS[table])
    
    try:
        # Ensure the materialized view exists before creating the unique index
        command = f"""
        BEGIN;
            CREATE MATERIALIZED VIEW IF NOT EXISTS public.{table}
        AS 
            SELECT DISTINCT ON ({index_columns}) *
            FROM (
                SELECT * FROM indexer.{table} WHERE chain_id != 11155111
                UNION ALL
                SELECT * FROM static_indexer_chain_data_75.{table} WHERE chain_id != 11155111
            ) combined_data;
        COMMIT;
        """
        execute_command(command)
        
        ensure_unique_index(table)  # This can raise an exception if there is an issue with the index creation
        
        command = f"""
        BEGIN;
            REFRESH MATERIALIZED VIEW CONCURRENTLY public.{table};
        COMMIT;
        """
        execute_command(command)  # This can raise an exception if there is an issue with the SQL execution

    except Exception as e:
        logger.warning(f"Failed to refresh materialized view {table}: {e}")
        command = f"""
        BEGIN;
            CREATE MATERIALIZED VIEW IF NOT EXISTS public.{table}
        AS 
            SELECT DISTINCT ON ({index_columns}) *
            FROM (
                SELECT * FROM indexer.{table} WHERE chain_id != 11155111
                UNION ALL
                SELECT * FROM static_indexer_chain_data_75.{table} WHERE chain_id != 11155111
            ) combined_data;
        COMMIT;
        """
        execute_command(command)  # This can also raise an exception if there is an issue with the SQL execution

        
def main():
    tables = ['applications_payouts', 'applications', 'rounds', 'donations']
    for table in tables:
        try:
            logger.info(f"Starting refresh for materialized view {table}")
            start_time = time.time()
            update_matview(table)
            end_time = time.time()
            logger.info(f"Successfully refreshed materialized view {table} in {end_time - start_time} seconds")
        except Exception as e:
            logger.error(f"Failed to refresh materialized view {table}: {e}", exc_info=True)

if __name__ == "__main__":
    main()