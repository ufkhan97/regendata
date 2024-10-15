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
TABLES_TO_DROP = ['rounds', 'donations', 'applications_payouts', 'applications']
TABLES_TO_IMPORT = ['rounds', 'donations', 'applications_payouts']
APPLICATIONS_TABLE_DEFINITION = """
CREATE FOREIGN TABLE indexer.applications (
  id text OPTIONS (column_name 'id') COLLATE pg_catalog."default" NOT NULL,
  chain_id integer OPTIONS (column_name 'chain_id') NOT NULL,
  round_id text OPTIONS (column_name 'round_id') COLLATE pg_catalog."default" NOT NULL,
  project_id text OPTIONS (column_name 'project_id') COLLATE pg_catalog."default",
  anchor_address text OPTIONS (column_name 'anchor_address') COLLATE pg_catalog."default",
  status text OPTIONS (column_name 'status'),  -- Simplified to text for testing
  status_snapshots jsonb OPTIONS (column_name 'status_snapshots'),
  distribution_transaction text OPTIONS (column_name 'distribution_transaction') COLLATE pg_catalog."default",
  metadata_cid text OPTIONS (column_name 'metadata_cid') COLLATE pg_catalog."default",
  metadata jsonb OPTIONS (column_name 'metadata'),
  created_by_address text OPTIONS (column_name 'created_by_address') COLLATE pg_catalog."default",
  created_at_block numeric(78,0) OPTIONS (column_name 'created_at_block'),
  status_updated_at_block numeric(78,0) OPTIONS (column_name 'status_updated_at_block'),
  total_donations_count integer OPTIONS (column_name 'total_donations_count'),
  total_amount_donated_in_usd real OPTIONS (column_name 'total_amount_donated_in_usd'),
  unique_donors_count integer OPTIONS (column_name 'unique_donors_count'),
  tags text[] OPTIONS (column_name 'tags') COLLATE pg_catalog."default"
)
SERVER indexer
OPTIONS (schema_name '{schema}', table_name 'applications');
"""

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


def drop_foreign_tables(tables, db_params):
    """Drop specified foreign tables."""
    for table in tables:
        drop_command = f'DROP FOREIGN TABLE IF EXISTS indexer.{table} CASCADE;'
        db.execute_command(drop_command, db_params, logger)


def create_applications_table(schema, db_params, logger=None):
    """Create the applications table."""
    create_command = APPLICATIONS_TABLE_DEFINITION.format(schema=schema)
    db.execute_command(create_command, db_params, logger)

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
        # Change this value to update the schema version
        latest_schema_version = 86
        schema_name = f'chain_data_{latest_schema_version}'
        drop_foreign_tables(TABLES_TO_DROP, DB_PARAMS)
        db.import_foreign_schema(server, schema_name, TABLES_TO_IMPORT, DB_PARAMS, target_schema, logger=logger)
        create_applications_table(schema_name, DB_PARAMS, logger)
        logger.info(f"Schema update completed successfully to version {latest_schema_version}.")
    except Exception as e:
        logger.error(f"Schema update failed: {e}")
        print("Schema update failed.")

if __name__ == "__main__":
    main()
