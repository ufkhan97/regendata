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
    'contributions',
    'donations'
]

TABLES_TO_IMPORT = [
    'rounds',
    'contributions'
]

TABLES_TO_CREATE_FROM_DEFINITION = [
    'applications',
    'round_roles'
]
TABLE_DEFINITIONS = {
    'applications': """
    CREATE FOREIGN TABLE {target_schema}.applications (
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
    SERVER {server}
    OPTIONS (schema_name '{schema}', table_name 'applications');
    """,
    'round_roles': """
    CREATE FOREIGN TABLE {target_schema}.round_roles (
      chain_id integer OPTIONS (column_name 'chain_id') NOT NULL,
      round_id text OPTIONS (column_name 'round_id') NOT NULL,
      address text OPTIONS (column_name 'address') NOT NULL,
      role text OPTIONS (column_name 'role') NOT NULL,
      created_at_block numeric(78,0) OPTIONS (column_name 'created_at_block')
    )
    SERVER {server}
    OPTIONS (schema_name '{schema}', table_name 'round_roles');
    """
}


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

def import_foreign_schema(schema, tables, db_params, server, target_schema='public'):
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
    FROM SERVER {server}
    INTO {target_schema}
    OPTIONS (import_default 'true');
    """
    logger.info(f"Importing foreign schema {schema} into local schema {target_schema} from server {server} limited to tables: {table_list}")
    execute_command(import_command, db_params)


def create_table_from_definition(table_name, schema, target_schema, server, db_params, logger=None):
    """Create a table from its definition."""
    logger.info(f"Creating local {target_schema} table {table_name} from foreign {server} schema {schema}")
    table_definition = TABLE_DEFINITIONS.get(table_name).format(schema=schema, target_schema=target_schema, server=server)
    if table_definition is None:
        raise ValueError(f"No definition found for table {table_name}")
    create_command = table_definition.format(schema=schema)
    db.execute_command(create_command, db_params, logger)

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
    server = 'maci'
    try:
        version_result = run_query(version_query, MACI_DB_PARAMS)
        latest_schema_version = version_result['latest_schema_version'][0]
        schema_name = f'chain_data_{latest_schema_version}'
        print(f"LATEST SCHEMA IS {schema_name}")
        drop_foreign_tables(TABLES_TO_DROP, target_schema, DB_PARAMS)
        import_foreign_schema(schema_name, TABLES_TO_IMPORT, DB_PARAMS, server, target_schema)
        for table in TABLES_TO_CREATE_FROM_DEFINITION:
            create_table_from_definition(table, schema_name, target_schema, server, DB_PARAMS, logger)
        logger.info(f"Schema {target_schema} update completed successfully to version {latest_schema_version}.")
    except Exception as e:
        logger.error(f"Schema update failed: {e}")
        print("Schema update failed.")

if __name__ == "__main__":
    main()
