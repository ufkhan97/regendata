## TO DO: IMPORT INTO ALL AUTOMATIONS AND SCRIPTS

import pandas as pd
import logging
import psycopg2 as pg

def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    return logging.getLogger(__name__)

def execute_command(command, DB_PARAMS, logger=None):
    if logger is None:
        logger = setup_logging()
    logger.info(f"Executing command: {command[:500]}...")  # Log first 50 characters
    connection = None
    try:
        connection = pg.connect(**DB_PARAMS)
        cursor = connection.cursor()
        cursor.execute("SET tcp_keepalives_idle = 180;")  # 3 minutes
        cursor.execute("SET tcp_keepalives_interval = 60;")  # 60 seconds
        cursor.execute(command)
        connection.commit()
        logger.info("Command executed successfully.")
    except pg.Error as e:
        logger.error(f"Database error: {e}")
        if connection:
            connection.rollback()
        raise
    finally:
        if connection:
            connection.close()

def run_query(query, db_params, logger=None):
    """Run a query and return the results as a DataFrame."""
    if logger is None:
        logger = setup_logging()
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
    

def import_foreign_schema(server, schema, tables, db_params, target_schema='public', logger=None):
    """
    Import specified tables from a foreign schema with optional renaming.
    
    :param server: The name of the foreign server
    :param schema: The name of the schema to import from
    :param tables: List of table names to import
    :param db_params: Database connection parameters
    :param target_schema: The name of the schema to import into (default: 'public')
    :param logger: Logger object (optional)
    """
    if logger is None:
        logger = setup_logging()

    table_list = ', '.join(f'"{table}"' for table in tables)

    # Create the target schema if it doesn't exist
    create_schema_command = f'CREATE SCHEMA IF NOT EXISTS "{target_schema}";'
    execute_command(create_schema_command, db_params, logger)

    import_command = f"""
    IMPORT FOREIGN SCHEMA "{schema}"
    LIMIT TO ({table_list})
    FROM SERVER indexer
    INTO "{target_schema}";
    """
    execute_command(import_command, db_params, logger)