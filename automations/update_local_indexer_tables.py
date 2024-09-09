import os
import psycopg2
import logging
import time
from decimal import Decimal

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
MATVIEW_CONFIGS = {
    'applications': {
        'index_columns': ['id', 'chain_id', 'round_id'],
        'order_by': 'id DESC, chain_id DESC, round_id DESC',
        'amount_column': None
    },
    'applications_payouts': {
        'index_columns': ['id'],
        'order_by': 'id DESC',
        'amount_column': 'amount_in_usd'
    },
    'rounds': {
        'index_columns': ['id', 'chain_id'],
        'order_by': 'id DESC, chain_id DESC',
        'amount_column': 'total_amount_donated_in_usd + CASE WHEN matching_distribution IS NOT NULL THEN match_amount_in_usd ELSE 0 END'
    },
    'donations': {
        'index_columns': ['id'],
        'order_by': 'id DESC',
        'amount_column': 'amount_in_usd'
    }
}

def get_connection():
    try:
        return psycopg2.connect(**DB_PARAMS)
    except psycopg2.Error as e:
        logger.error(f"Failed to connect to the database: {e}")
        raise

def execute_command(connection, command):
    logger.info(f"Executing command: {command[:50]}...")
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

def get_matview_total(connection, matview):
    config = MATVIEW_CONFIGS[matview]
    amount_column = config['amount_column']
    
    if not amount_column:
        return None
    
    query = f"SELECT SUM({amount_column}) FROM public.{matview}"
    
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchone()
            return result[0] if result else Decimal('0')
    except psycopg2.Error as e:
        logger.error(f"Error fetching matview total for {matview}: {e}")
        return None

def refresh_matview(connection, matview):
    config = MATVIEW_CONFIGS[matview]
    index_columns = ', '.join(config['index_columns'])
    order_by = config['order_by']
    
    try:
        old_matview_total = get_matview_total(connection, matview)

        refresh_command = f"""
        CREATE OR REPLACE FUNCTION refresh_{matview}()
        RETURNS VOID AS $$
        BEGIN
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

            DROP MATERIALIZED VIEW IF EXISTS public.{matview} CASCADE;
            ALTER MATERIALIZED VIEW public.{matview}_new RENAME TO {matview};
        END;
        $$ LANGUAGE plpgsql;

        SELECT refresh_{matview}();
        """
        execute_command(connection, refresh_command)

        index_name = f"{matview}_unique_idx"
        index_command = f"""
        CREATE UNIQUE INDEX IF NOT EXISTS {index_name} ON public.{matview} ({index_columns});
        """
        execute_command(connection, index_command)

        new_matview_total = get_matview_total(connection, matview)

        if old_matview_total is not None and new_matview_total is not None:
            if new_matview_total < old_matview_total: 
                logger.warning(f"Total amount for {matview} has decreased from {old_matview_total} to {new_matview_total}")
            logger.info(f"Materialized view {matview} total amount changed from {old_matview_total} to {new_matview_total}")

    except Exception as e:
        logger.error(f"Failed to refresh materialized view {matview}: {e}", exc_info=True)
        raise

def main():
    connection = None
    try:
        connection = get_connection()
        for matview in [mv for mv in MATVIEW_CONFIGS if mv != 'donations']:
            logger.info(f"Starting refresh for materialized view {matview}")
            start_time = time.time()
            refresh_matview(connection, matview)
            end_time = time.time()
            logger.info(f"Finished refresh for materialized view {matview} in {end_time - start_time:.2f} seconds")
        
        logger.info("Starting refresh for materialized view donations")
        start_time = time.time()
        refresh_matview(connection, 'donations')
        end_time = time.time()
        logger.info(f"Finished refresh for materialized view donations in {end_time - start_time:.2f} seconds")
    except Exception as e:
        logger.error(f"An error occurred during execution: {e}", exc_info=True)
    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    main()