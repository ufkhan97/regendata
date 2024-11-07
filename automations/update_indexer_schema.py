import os
import psycopg2 as pg
import pandas as pd
import logging
import db_utils as db

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


# Constants for table names
TABLES_TO_DROP = [
    'round_roles',
    'applications',
    'rounds',
    'donations',
    'applications_payouts'
]

TABLES_TO_IMPORT = [
    'rounds',
    'donations',
    'applications_payouts'
]

TABLES_TO_CREATE_FROM_DEFINITION = [
    'round_roles',
    'applications'
]
TABLE_DEFINITIONS = {
    'applications': """
    CREATE FOREIGN TABLE indexer.applications (
      id text OPTIONS (column_name 'id') NOT NULL,
      chain_id integer OPTIONS (column_name 'chain_id') NOT NULL,
      round_id text OPTIONS (column_name 'round_id') NOT NULL,
      project_id text OPTIONS (column_name 'project_id'),
      anchor_address text OPTIONS (column_name 'anchor_address'),
      status text OPTIONS (column_name 'status'),
      status_snapshots jsonb OPTIONS (column_name 'status_snapshots'),
      distribution_transaction text OPTIONS (column_name 'distribution_transaction'),
      metadata_cid text OPTIONS (column_name 'metadata_cid'),
      metadata jsonb OPTIONS (column_name 'metadata'),
      created_by_address text OPTIONS (column_name 'created_by_address'),
      created_at_block numeric(78,0) OPTIONS (column_name 'created_at_block'),
      status_updated_at_block numeric(78,0) OPTIONS (column_name 'status_updated_at_block'),
      total_donations_count integer OPTIONS (column_name 'total_donations_count'),
      total_amount_donated_in_usd real OPTIONS (column_name 'total_amount_donated_in_usd'),
      unique_donors_count integer OPTIONS (column_name 'unique_donors_count'),
      tags text[] OPTIONS (column_name 'tags')
    )
    SERVER indexer
    OPTIONS (schema_name '{schema}', table_name 'applications');
    """,
    'round_roles': """
    CREATE FOREIGN TABLE indexer.round_roles (
      chain_id integer OPTIONS (column_name 'chain_id') NOT NULL,
      round_id text OPTIONS (column_name 'round_id') NOT NULL,
      address text OPTIONS (column_name 'address') NOT NULL,
      role text OPTIONS (column_name 'role') NOT NULL,
      created_at_block numeric(78,0) OPTIONS (column_name 'created_at_block')
    )
    SERVER indexer
    OPTIONS (schema_name '{schema}', table_name 'round_roles');
    """
}

# Database connection details
INDEXER_DB_PARAMS = {
    'host': os.getenv('INDEXER_DB_HOST'),
    'port': os.getenv('INDEXER_DB_PORT'),
    'dbname': os.getenv('INDEXER_DB_NAME'),
    'user': os.getenv('INDEXER_DB_USER'),
    'password': os.getenv('INDEXER_DB_PASSWORD')
}

DB_PARAMS = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'dbname': 'Grants',
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

def run_query(query, db_params):
    """Run a query and return the results as a DataFrame."""
    try:
        with pg.connect(**db_params) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                col_names = [desc[0] for desc in cur.description]
                results = pd.DataFrame(cur.fetchall(), columns=col_names)
                return results
    except pg.Error as e:
        logger.error(f"ERROR: Could not execute the query. {e}")
        return None

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

def drop_foreign_tables(tables, schema, db_params):
    """Drop specified foreign tables from the given schema."""
    for table in tables:
        drop_command = f'DROP FOREIGN TABLE IF EXISTS {schema}.{table} CASCADE;'
        execute_command(drop_command, db_params)

def create_table_from_definition(table_name, schema, db_params, logger=None):
    """Create a table from its definition."""
    logger.info(f"Creating table {table_name} in schema {schema}")
    table_definition = TABLE_DEFINITIONS.get(table_name)
    if table_definition is None:
        raise ValueError(f"No definition found for table {table_name}")
    create_command = table_definition.format(schema=schema)
    db.execute_command(create_command, db_params, logger)


def check_table_exists(schema, table, db_params):
    query = f"""
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = '{schema}'
        AND table_name = '{table}'
    );
    """
    result = db.run_query(query, db_params, logger)
    logger.info(f"Table {table} exists in {schema}: {result}")


# Main execution logic
def main():
    version_query = '''
    SELECT
       max(substr(table_schema, 12)::int) as latest_schema_version
    FROM
       information_schema.tables
    WHERE table_schema LIKE 'chain_data_%';
    '''
    try:
        server = 'indexer'
        target_schema = 'indexer'

        # Uncomment the following lines to automatically fetch the latest schema version
        # version_result = db.run_query(version_query, INDEXER_DB_PARAMS, logger)
        # latest_schema_version = version_result['latest_schema_version'][0]

        # Manual override for schema version
        latest_schema_version = 86 # Change this value to update the schema version
        schema_name = f'chain_data_{latest_schema_version}'

        drop_foreign_tables(TABLES_TO_DROP, target_schema, DB_PARAMS)
        db.import_foreign_schema(server, schema_name, TABLES_TO_IMPORT, DB_PARAMS, target_schema, logger=logger)
        for table in TABLES_TO_CREATE_FROM_DEFINITION:
            create_table_from_definition(table, schema_name, DB_PARAMS, logger)
        logger.info(f"Schema update completed successfully to version {latest_schema_version}.")
    except Exception as e:
        logger.error(f"Schema update failed: {e}")
        print("Schema update failed.")

if __name__ == "__main__":
    main()