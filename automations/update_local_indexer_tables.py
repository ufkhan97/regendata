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
    # Define unique index columns for each table
    index_columns = {
        'applications': ['id', 'chain_id', 'round_id'],
        'rounds': ['id', 'chain_id'],
        'donations': ['id']  # Adjust these columns as needed
    }
    
    columns = index_columns.get(table)
    if not columns:
        logger.warning(f"No unique index defined for table {table}")
        return

    index_name = f"{table}_local_unique_idx"
    column_list = ', '.join(columns)
    
    command = f"""
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1
            FROM pg_indexes
            WHERE schemaname = 'experimental_views'
            AND tablename = '{table}_local'
            AND indexname = '{index_name}'
        ) THEN
            CREATE UNIQUE INDEX {index_name} ON experimental_views.{table}_local ({column_list});
        END IF;
    END $$;
    """
    execute_command(command)
    return 

def update_matview(table):
    user = DB_PARAMS['user']
    
    try:
        ensure_unique_index(table)
        command = f"""
        BEGIN;
            REFRESH MATERIALIZED VIEW CONCURRENTLY experimental_views.{table}_local;
            GRANT USAGE ON SCHEMA experimental_views TO {user};
            GRANT SELECT ON experimental_views.{table}_local TO {user};
        COMMIT;
        """
    except Exception as e:
        logger.warning(f"Failed to ensure unique index for table {table}: {e}")
        command = f"""
        BEGIN;
            CREATE MATERIALIZED VIEW IF NOT EXISTS experimental_views.{table}_local
        AS SELECT * FROM public.{table};
        
            GRANT USAGE ON SCHEMA experimental_views TO {user};
            GRANT SELECT ON experimental_views.{table}_local TO {user};
        COMMIT;
        """
    finally:
        execute_command(command)

def main():
    tables = ['applications', 'rounds', 'donations']
    for table in tables:
        try:
            logger.info(f"Starting refresh for materialized view {table}_local")
            start_time = time.time()
            update_matview(table)
            end_time = time.time()
            logger.info(f"Successfully refreshed materialized view {table}_local in {end_time - start_time} seconds")
        except Exception as e:
            logger.error(f"Failed to refresh materialized view {table}_local: {e}", exc_info=True)

if __name__ == "__main__":
    main()