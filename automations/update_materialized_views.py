import os
import psycopg2
import logging
import time
from decimal import Decimal
from typing import Dict, Optional, List

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Try to load .env file if it exists (for local development)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Database configuration
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
BASE_MATVIEWS = {
    'applications': {
        'index_columns': ['id', 'chain_id', 'round_id'],
        'order_by': 'id DESC, chain_id DESC, round_id DESC',
        'amount_column': None
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
     },
    'applications_payouts': {
        'index_columns': ['id'],
        'order_by': 'id DESC',
        'amount_column': 'amount_in_usd'
    },
    'round_roles': {
        'index_columns': ['chain_id', 'round_id', 'address', 'role'],
        'order_by': 'chain_id DESC, round_id DESC, address DESC, role DESC',
        'amount_column': None
    }
}

DEPENDENT_MATVIEWS = {
    'indexer_matching': {
        'query_file': 'automations/queries/indexer_matching.sql',
        'amount_column': 'match_amount_in_usd',
        'schema': 'public'  
    },
    'all_donations': {
        'query_file': 'automations/queries/all_donations.sql',
        'amount_column': 'amount_in_usd',
        'schema': 'public'
    },
    'all_matching': {
        'query_file': 'automations/queries/all_matching.sql',
        'amount_column': 'match_amount_in_usd',
        'schema': 'public'
    },
    'allo_gmv_leaderboard_events': {
        'query_file': 'automations/queries/allo_gmv_with_ens.sql',
        'amount_column': 'gmv',
        'schema': 'experimental_views'
    }
}

def get_connection():
    """Establish database connection with proper settings."""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        with conn.cursor() as cursor:
            # Set keepalive parameters
            cursor.execute("SET tcp_keepalives_idle = 180;")  # 3 minutes
            cursor.execute("SET tcp_keepalives_interval = 60;")  # 60 seconds
        return conn
    except psycopg2.Error as e:
        logger.error(f"Failed to connect to the database: {e}")
        raise

def execute_command(connection, command: str, params: tuple = None) -> None:
    """Execute a database command with proper error handling."""
    logger.info(f"Executing command: {command[:100]}...")
    try:
        with connection.cursor() as cursor:
            if params:
                cursor.execute(command, params)
            else:
                cursor.execute(command)
        connection.commit()
        logger.info("Command executed successfully.")
    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        connection.rollback()
        raise

def get_matview_total(connection, matview: str, config: dict, schema: str = 'public') -> Optional[Decimal]:
    amount_column = config.get('amount_column')
    if not amount_column:
        return None 

    query = f"""
    SELECT SUM({amount_column})
    FROM {schema}.{matview}
    WHERE {amount_column} IS NOT NULL
    """
    
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchone()
            return result[0] if result and result[0] else Decimal('0')
    except psycopg2.Error as e:
        logger.error(f"Error fetching matview total for {schema}.{matview}: {e}")
        return None

def create_base_matview(connection, matview: str, config: dict) -> None:
    """Create a new base materialized view."""
    index_columns = ', '.join(config['index_columns'])
    
    create_command = f"""
    DROP MATERIALIZED VIEW IF EXISTS public.{matview}_new CASCADE;
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

def create_dependent_matview(connection, matview: str, config: dict) -> None:
    """Create a new dependent materialized view."""
    query_file = config['query_file']
    schema = config.get('schema', 'public')  # default to public if not specified
    
    with open(query_file, 'r') as file:
        query = file.read()
        
    # Replace references to both base and dependent views with their _new versions
    for view in list(BASE_MATVIEWS.keys()) + list(DEPENDENT_MATVIEWS.keys()):
        # Check and replace "FROM view" patterns
        from_pattern = f"FROM {view} "
        if from_pattern in query:
            position = query.find(from_pattern)
            print(f"Original query segment: '{query[position-50:position+50]}'")
            print(f"Replacing '{from_pattern}' with 'FROM {view}_new '")
            query = query.replace(from_pattern, f"FROM {view}_new ")
            position = query.find(f"FROM {view}_new")
            print(f"Updated query segment: '{query[position-50:position+50]}'")
        
        # Check and replace "JOIN view" patterns
        join_pattern = f"JOIN {view} "
        if join_pattern in query:
            position = query.find(join_pattern)
            print(f"Original query segment: '{query[position-50:position+50]}'")
            print(f"Replacing '{join_pattern}' with 'JOIN {view}_new '")
            query = query.replace(join_pattern, f"JOIN {view}_new ")
            position = query.find(f"JOIN {view}_new")
            print(f"Updated query segment: '{query[position-50:position+50]}'")
    
    create_command = f"""
    DROP MATERIALIZED VIEW IF EXISTS {schema}.{matview}_new CASCADE;
    
    CREATE MATERIALIZED VIEW {schema}.{matview}_new AS
    {query}
    """
    print(create_command)
    execute_command(connection, create_command)

def create_indexes(connection, matview: str, config: dict) -> None:
    """Create indexes for a materialized view."""
    if 'index_columns' in config:
        index_columns = ', '.join(config['index_columns'])
        index_name = f"{matview}_idx"
        
        index_command = f"""
        CREATE UNIQUE INDEX IF NOT EXISTS {index_name}
        ON public.{matview} ({index_columns});
        """
        
        execute_command(connection, index_command)

def validate_refresh(connection, matview: str, config: dict, old_total: Optional[Decimal]) -> None:
    """Validate the refresh operation for a materialized view."""
    new_total = get_matview_total(connection, matview, config)
    
    if old_total is not None and new_total is not None:
        if new_total < old_total:
            logger.warning(
                f"Total amount for {matview} has decreased: {old_total} -> {new_total}"
            )
        elif new_total > old_total:
            logger.info(
                f"Total amount for {matview} has increased: {old_total} -> {new_total}"
            )
        else:
            logger.info(f"Total amount for {matview} remains unchanged at {new_total}")

def validate_view_exists(connection, schema: str, matview: str) -> bool:
    """Validate that a materialized view exists"""
    check_query = """
    SELECT EXISTS (
        SELECT FROM pg_matviews 
        WHERE schemaname = %s 
        AND matviewname = %s
    );
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute(check_query, (schema, matview))
            exists = cursor.fetchone()[0]
            if not exists:
                logger.error(f"Materialized view {schema}.{matview} does not exist")
            return exists
    except psycopg2.Error as e:
        logger.error(f"Error checking materialized view existence: {e}")
        return False


def refresh_materialized_views(connection) -> None:
    """Refresh all materialized views while maintaining dependencies."""
    try:
        # Step 1: Store current totals for validation (base views only)
        logger.info("Recording current totals...")
        old_totals = {}
        for matview, config in BASE_MATVIEWS.items():
            old_totals[matview] = get_matview_total(connection, matview, config)


        # Step 2: Create all new base views
        logger.info("Creating new base materialized views...")
        for matview, config in BASE_MATVIEWS.items():
            logger.info(f"Creating {matview}_new...")
            create_base_matview(connection, matview, config)
            create_indexes(connection, f"{matview}_new", config)

        # Step 3: Create all new dependent views
        logger.info("Creating new dependent materialized views...")
        for matview, config in DEPENDENT_MATVIEWS.items():
            logger.info(f"Creating {matview}_new...")
            create_dependent_matview(connection, matview, config)
            if 'index_columns' in config:
                create_indexes(connection, f"{matview}_new", config)

        # Step 4: Atomic swap of all views
        logger.info("Performing atomic swap of all views...")
        swap_commands = ["BEGIN;"]
        
        # Add swap commands for all views
        for matview, config in BASE_MATVIEWS.items():
            schema = 'public'  # base views are always in public
            swap_commands.extend([
                f"DROP MATERIALIZED VIEW IF EXISTS {schema}.{matview}_old CASCADE;",
                f"ALTER MATERIALIZED VIEW IF EXISTS {schema}.{matview} RENAME TO {matview}_old;",
                f"ALTER MATERIALIZED VIEW {schema}.{matview}_new RENAME TO {matview};"
            ])

        for matview, config in DEPENDENT_MATVIEWS.items():
            schema = config.get('schema', 'public')
            swap_commands.extend([
                f"DROP MATERIALIZED VIEW IF EXISTS {schema}.{matview}_old CASCADE;",
                f"ALTER MATERIALIZED VIEW IF EXISTS {schema}.{matview} RENAME TO {matview}_old;",
                f"ALTER MATERIALIZED VIEW {schema}.{matview}_new RENAME TO {matview};"
            ])
        
        swap_commands.append("COMMIT;")
        execute_command(connection, "\n".join(swap_commands))

        # Step 5: Validate
        # Need to modify validation to handle schema...
        logger.info("Validating refreshed views...")
        for matview, config in BASE_MATVIEWS.items():
            validate_refresh(connection, matview, config, old_totals[matview])
            
        for matview, config in DEPENDENT_MATVIEWS.items():
            schema = config.get('schema', 'public')
            new_total = get_matview_total(connection, matview, config, schema)
            logger.info(f"New dependent view {schema}.{matview} total: {new_total}")

        # Step 6: Cleanup
        logger.info("Cleaning up old views...")
        cleanup_commands = []
        for matview, config in BASE_MATVIEWS.items():
            cleanup_commands.append(
                f"DROP MATERIALIZED VIEW IF EXISTS public.{matview}_old CASCADE;"
            )
        for matview, config in DEPENDENT_MATVIEWS.items():
            schema = config.get('schema', 'public')
            cleanup_commands.append(
                f"DROP MATERIALIZED VIEW IF EXISTS {schema}.{matview}_old CASCADE;"
            )
        execute_command(connection, "\n".join(cleanup_commands))
        logger.info("Materialized view refresh completed successfully.")

    except Exception as e:
        logger.error(f"Failed to refresh materialized views: {e}", exc_info=True)
        raise


def main():
    """Main execution function."""
    connection = None
    start_time = time.time()
    
    try:
        connection = get_connection()
        refresh_materialized_views(connection)
        
        end_time = time.time()
        logger.info(f"Total refresh time: {end_time - start_time:.2f} seconds")

        for matview, config in DEPENDENT_MATVIEWS.items():
            schema = config.get('schema', 'public')
            validate_view_exists(connection, schema, matview)
        
    except Exception as e:
        logger.error(f"An error occurred during execution: {e}", exc_info=True)
        raise
    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    main()
