import os
import psycopg2 as pg
import pandas as pd
import logging
import db_utils as db
from typing import List, Dict, Optional

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

class DatabaseConfig:
    def __init__(self, name: str, tables_config: Dict, db_params: Dict, default_version: Optional[int] = None):
        self.name = name
        self.server = name
        self.schema = name
        self.tables_to_drop = tables_config.get('drop', [])
        self.tables_to_import = tables_config.get('import', [])
        self.tables_to_create = tables_config.get('create', [])
        self.db_params = db_params
        self.default_version = default_version  # Allow setting a default schema version

# Configuration for different database targets
MACI_CONFIG = DatabaseConfig(
    name='maci',
    default_version=None,  # Will use auto-detection
    tables_config={
        'drop': [
            'round_roles',
            'applications',
            'rounds',
            'contributions',
            'donations'
        ],
        'import': [
            'rounds',
            'contributions'
        ],
        'create': [
            'applications',
            'round_roles'
        ]
    },
    db_params={
        'host': os.getenv('MACI_DB_HOST'),
        'port': os.getenv('MACI_DB_PORT'),
        'dbname': os.getenv('MACI_DB_NAME'),
        'user': os.getenv('MACI_DB_USER'),
        'password': os.getenv('MACI_DB_PASSWORD')
    }
)

INDEXER_CONFIG = DatabaseConfig(
    name='indexer',
    tables_config={
        'drop': [
            'round_roles',
            'applications',
            'rounds',
            'donations',
            'applications_payouts'
        ],
        'import': [
            'rounds',
            'donations',
            'applications_payouts'
        ],
        'create': [
            'round_roles',
            'applications'
        ]
    },
    db_params={
        'host': os.getenv('INDEXER_DB_HOST'),
        'port': os.getenv('INDEXER_DB_PORT'),
        'dbname': os.getenv('INDEXER_DB_NAME'),
        'user': os.getenv('INDEXER_DB_USER'),
        'password': os.getenv('INDEXER_DB_PASSWORD')
    }
)

# Common database parameters
DB_PARAMS = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'dbname': 'Grants',
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

# Table definitions template with parameterized schema and server
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

def run_query(query: str, db_params: Dict) -> Optional[pd.DataFrame]:
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

def execute_command(command: str, db_params: Dict) -> None:
    """Execute a SQL command that doesn't return results."""
    try:
        with pg.connect(**db_params) as conn:
            with conn.cursor() as cur:
                cur.execute(command)
                conn.commit()
                logger.info("Command executed successfully.")
    except pg.Error as e:
        logger.error(f"ERROR: Could not execute the command. {e}")

def drop_foreign_tables(tables: List[str], schema: str, db_params: Dict) -> None:
    """Drop specified foreign tables from the given schema."""
    for table in tables:
        drop_command = f'DROP FOREIGN TABLE IF EXISTS {schema}.{table} CASCADE;'
        execute_command(drop_command, db_params)

def import_foreign_schema(schema: str, tables: List[str], db_params: Dict, server: str, target_schema: str = 'public') -> None:
    """Import specified tables from a foreign schema."""
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
    logger.info(f"Importing foreign schema {schema} into local schema {target_schema} from server {server}")
    execute_command(import_command, db_params)

def create_table_from_definition(
    table_name: str,
    schema: str,
    target_schema: str,
    server: str,
    db_params: Dict
) -> None:
    """Create a table from its definition."""
    logger.info(f"Creating table {table_name} in schema {target_schema}")
    table_definition = TABLE_DEFINITIONS.get(table_name)
    if table_definition is None:
        raise ValueError(f"No definition found for table {table_name}")
    create_command = table_definition.format(
        schema=schema,
        target_schema=target_schema,
        server=server
    )
    execute_command(create_command, db_params)

def get_latest_schema_version(db_params: Dict) -> Optional[int]:
    """Get the latest schema version from the database, supporting 2 or 3 digit versions."""
    version_query = '''
    SELECT
       max(substr(table_schema, 12)::int) as latest_schema_version
    FROM
       information_schema.tables
    WHERE table_schema LIKE 'chain_data___' OR table_schema LIKE 'chain_data____';
    '''
    version_result = run_query(version_query, db_params)
    if version_result is None or version_result.empty:
        logger.error("No schema version found in the database")
        return None
    
    version = version_result['latest_schema_version'][0]
    logger.info(f"Found latest schema version: {version}")
    return version

def update_schema(config: DatabaseConfig, schema_version: Optional[int] = None) -> None:
    """Update schema for a specific database configuration."""
    try:
        if schema_version is None:
            schema_version = get_latest_schema_version(config.db_params)
            if schema_version is None:
                raise ValueError(f"Could not determine schema version for {config.name}")
        
        schema_name = f'chain_data_{schema_version}'
        logger.info(f"Updating {config.name} schema to version {schema_version}")
        
        # Drop existing tables
        drop_foreign_tables(config.tables_to_drop, config.schema, DB_PARAMS)
        
        # Import specified tables
        import_foreign_schema(
            schema_name,
            config.tables_to_import,
            DB_PARAMS,
            config.server,
            config.schema
        )
        
        # Create tables from definitions
        for table in config.tables_to_create:
            create_table_from_definition(
                table,
                schema_name,
                config.schema,
                config.server,
                DB_PARAMS
            )
            
        logger.info(f"Schema update completed successfully for {config.name}")
        
    except Exception as e:
        logger.error(f"Schema update failed for {config.name}: {e}")
        raise

def test_connection(db_params: Dict, name: str) -> bool:
    """Test database connection and list available schemas."""
    try:
        with pg.connect(**db_params) as conn:
            with conn.cursor() as cur:
                # Test basic connection
                cur.execute("SELECT current_database(), current_user;")
                db, user = cur.fetchone()
                logger.info(f"Connected to {name} database: {db} as user: {user}")
                
                # List schemas matching our pattern
                cur.execute("""
                    SELECT table_schema 
                    FROM information_schema.tables 
                    WHERE table_schema LIKE 'chain_data_%'
                    GROUP BY table_schema 
                    ORDER BY table_schema DESC 
                    LIMIT 5;
                """)
                schemas = [row[0] for row in cur.fetchall()]
                logger.info(f"Found recent chain_data schemas in {name}: {schemas}")
                return True
    except Exception as e:
        logger.error(f"Failed to connect to {name} database: {e}")
        return False

def main():
    """Main execution function."""
    # Test connections first
    maci_ok = test_connection(MACI_CONFIG.db_params, "MACI")
    indexer_ok = test_connection(INDEXER_CONFIG.db_params, "Indexer")
    try:
        # Update MACI schema
        #update_schema(MACI_CONFIG)
        
        # Update Indexer schema with specific version
        update_schema(INDEXER_CONFIG, schema_version=86)
        
    except Exception as e:
        logger.error(f"Schema update failed: {e}")
        print("Schema update failed.")

if __name__ == "__main__":
    main()