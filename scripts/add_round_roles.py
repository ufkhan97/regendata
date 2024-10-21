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
TABLES_TO_DROP = ['round_roles']
TABLES_TO_IMPORT = ['round_roles']

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

def test_db_connection(db_params, db_name):
    try:
        conn = pg.connect(**db_params)
        conn.close()
        print(f"Successfully connected to {db_name} database.")
    except Exception as e:
        print(f"Failed to connect to {db_name} database: {str(e)}")

def drop_foreign_tables(tables, db_params):
    """Drop specified foreign tables."""
    for table in tables:
        drop_command = f'DROP FOREIGN TABLE IF EXISTS indexer.{table} CASCADE;'
        db.execute_command(drop_command, db_params, logger)

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

def check_available_schemas(db_params):
    query = """
    SELECT schema_name 
    FROM information_schema.schemata 
    WHERE schema_name LIKE 'chain_data_%'
    ORDER BY schema_name DESC
    LIMIT 5;
    """
    result = db.run_query(query, db_params, logger)
    logger.info(f"Available schemas: {result}")

def check_table_structure(schema, table, db_params):
    query = f"""
    SELECT column_name, data_type, udt_name
    FROM information_schema.columns
    WHERE table_schema = '{schema}'
    AND table_name = '{table}'
    ORDER BY ordinal_position;
    """
    result = db.run_query(query, db_params, logger)
    logger.info(f"Structure of {schema}.{table}:\n{result}")

def check_foreign_server_permissions(db_params):
    query = """
    SELECT srvname, rolname
    FROM pg_foreign_server fs
    JOIN pg_user_mappings um ON fs.oid = um.srvid
    JOIN pg_roles r ON um.umuser = r.oid
    WHERE fs.srvname = 'indexer';
    """
    result = db.run_query(query, db_params, logger)
    logger.info(f"Foreign server permissions: {result}")

def check_indexer_schemas(db_params):
    query = """
    SELECT schema_name 
    FROM information_schema.schemata 
    WHERE schema_name LIKE 'chain_data_%'
    ORDER BY schema_name DESC
    LIMIT 5;
    """
    result = db.run_query(query, db_params, logger)
    logger.info(f"Available schemas on indexer: {result}")

def import_foreign_schema(server, schema_name, tables, db_params, target_schema, logger=None):
    for table in tables:
        command = f"""
        CREATE FOREIGN TABLE {target_schema}.{table} (
            chain_id integer NOT NULL,
            round_id text NOT NULL,
            address text NOT NULL,
            role text NOT NULL,
            created_at_block numeric(78,0)
        )
        SERVER {server}
        OPTIONS (schema_name '{schema_name}', table_name '{table}');
        """
        db.execute_command(command, db_params, logger)

def main():
    try:
        print("Starting schema update process...")
        
        test_db_connection(INDEXER_DB_PARAMS, "Indexer")
        test_db_connection(DB_PARAMS, "Local")

        server = 'indexer'
        target_schema = 'indexer'
        latest_schema_version = 86
        schema_name = f'chain_data_{latest_schema_version}'
        
        print(f"Checking available schemas...")
        check_available_schemas(INDEXER_DB_PARAMS)
        
        print(f"Checking if table exists in schema {schema_name}...")
        check_table_exists(schema_name, 'round_roles', INDEXER_DB_PARAMS)
        
        print(f"Checking table structure...")
        check_table_structure(schema_name, 'round_roles', INDEXER_DB_PARAMS)
        
        print("Checking foreign server permissions...")
        check_foreign_server_permissions(DB_PARAMS)
        
        print("Checking available schemas on indexer...")
        check_indexer_schemas(INDEXER_DB_PARAMS)
        
        print("Dropping foreign tables...")
        drop_foreign_tables(TABLES_TO_DROP, DB_PARAMS)
        
        print(f"Importing foreign schema {schema_name}...")
        import_foreign_schema(server, schema_name, TABLES_TO_IMPORT, DB_PARAMS, target_schema, logger)
        
        print(f"Schema update completed successfully to version {latest_schema_version}.")
    except Exception as e:
        print(f"Schema update failed: {str(e)}")
        logger.error(f"Schema update failed: {e}", exc_info=True)

if __name__ == "__main__":
    main()