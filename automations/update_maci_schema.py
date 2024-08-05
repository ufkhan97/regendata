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


# Constants for table names
TABLES_TO_DROP= ['rounds', 'contributions', 'applications']
TABLES_TO_IMPORT = ['rounds', 'contributions']
APPLICATIONS_TABLE_DEFINITION = """
CREATE FOREIGN TABLE maci.applications (
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
SERVER maci
OPTIONS (schema_name '{schema}', table_name 'applications');
"""

# Database connection details
MACI_DB_PARAMS = {
    'host': os.getenv('MACI_DB_HOST'),
    'port': os.getenv('MACI_DB_PORT'),
    'dbname': os.getenv('MACI_DB_NAME'),
    'user': os.getenv('MACI_DB_USER'),
    'password': os.getenv('MACI_DB_PASSWORD')
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

def import_foreign_schema(schema, tables, db_params, target_schema='public'):
    """
    Import specified tables from a foreign schema with optional renaming.
    
    :param schema: The name of the schema to import from
    :param tables: List of table names to import
    :param db_params: Database connection parameters
    :param target_schema: The name of the schema to import into (default: 'public')
    """
    table_list = ', '.join(tables)

    # Create the target schema if it doesn't exist
    create_schema_command = f"CREATE SCHEMA IF NOT EXISTS {target_schema};"
    execute_command(create_schema_command, db_params)
    
    import_command = f"""
    IMPORT FOREIGN SCHEMA {schema}
    LIMIT TO ({table_list})
    FROM SERVER maci
    INTO {target_schema}
    OPTIONS (import_default 'true');
    """
    execute_command(import_command, db_params)

def create_applications_table(schema, db_params):
    """Create the applications table."""
    create_command = APPLICATIONS_TABLE_DEFINITION.format(schema=schema)
    execute_command(create_command, db_params)

# Main execution logic
def main():
    version_query = '''
    SELECT
       max(substr(table_schema, 12)::int) as latest_schema_version
    FROM
       information_schema.tables
    WHERE table_schema LIKE 'chain_data___';
    '''
    target_schema = 'maci'
    try:
        version_result = run_query(version_query, MACI_DB_PARAMS)
        latest_schema_version = version_result['latest_schema_version'][0]
        schema_name = f'chain_data_{latest_schema_version}'
        print(f"LATEST SCHEMA IS {schema_name}")
        drop_foreign_tables(TABLES_TO_DROP, target_schema, DB_PARAMS)
        import_foreign_schema(schema_name, TABLES_TO_IMPORT, DB_PARAMS, target_schema)
        create_applications_table(schema_name, DB_PARAMS)
        logger.info(f"Schema {target_schema} update completed successfully to version {latest_schema_version}.")
    except Exception as e:
        logger.error(f"Schema update failed: {e}")
        print("Schema update failed.")

if __name__ == "__main__":
    main()
