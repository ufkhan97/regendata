import os
import json
import psycopg2 as pg
import pandas as pd
import logging
import subprocess
from typing import Dict, Optional, List
from datetime import datetime, timedelta
import requests


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
    def __init__(self, name: str, tables_config: Dict, db_params: Dict):
        self.name = name
        self.server = name
        self.schema = name
        self.tables_to_drop = tables_config.get('drop', [])
        self.tables_to_import = tables_config.get('import', [])
        self.tables_to_create = tables_config.get('create', [])
        self.db_params = db_params

# Configuration for different database targets
MACI_CONFIG = DatabaseConfig(
    name='maci',
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

# Table definitions
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

def load_schema_versions() -> Dict[str, Dict]:
    """Load current schema versions and last check time from JSON file."""
    try:
        with open('schema_versions.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.info("No existing schema_versions.json found, creating new one")
        return {
            "maci": {"version": None, "last_checked": None},
            "indexer": {"version": None, "last_checked": None}
        }

def save_schema_versions(versions: Dict[str, Dict]) -> None:
    """Save schema versions and last check time to JSON file and commit changes."""
    # Convert any numpy integers to Python integers
    converted_versions = {
        k: {
            "version": int(v["version"]) if v["version"] is not None else None,
            "last_checked": v["last_checked"]
        }
        for k, v in versions.items()
    }
    
    with open('schema_versions.json', 'w') as f:
        json.dump(converted_versions, f, indent=2)
    
    
    try:
        # Configure git
        subprocess.run(['git', 'config', '--local', 'user.email', 'github-actions[bot]@users.noreply.github.com'])
        subprocess.run(['git', 'config', '--local', 'user.name', 'github-actions[bot]'])
        
        # Stage and commit changes
        subprocess.run(['git', 'add', 'schema_versions.json'])
        result = subprocess.run(['git', 'commit', '-m', 'Update schema versions [skip ci]'],
                              capture_output=True, text=True)
        
        # Only push if there were actual changes
        if "nothing to commit" not in result.stdout:
            subprocess.run(['git', 'push'])
            logger.info("Committed and pushed schema version updates")
        else:
            logger.info("No changes to schema versions")
            
    except Exception as e:
        logger.error(f"Failed to commit schema version updates: {e}")

def should_check_schema(config_name: str) -> bool:
    """Determine if we should check for schema updates based on last check time."""
    versions = load_schema_versions()
    last_checked = versions[config_name]["last_checked"]
    
    if last_checked is None:
        return True
        
    last_check_time = datetime.fromisoformat(last_checked)
    time_since_check = datetime.now() - last_check_time
    
    if time_since_check < timedelta(hours=24):
        logger.info(f"Skipping {config_name} schema check - last checked {last_checked}")
        return False
    
    return True

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
    OPTIONS (import_default 'false');
    """
    logger.info(f"Importing foreign schema {schema} into local schema {target_schema}")
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

def update_schema(config: DatabaseConfig) -> Optional[int]:
    """Update schema for a specific database configuration. Returns the new version if updated."""
    try:
        # Check if we should proceed based on last check time
        if not should_check_schema(config.name):
            return None

        # Load existing versions
        current_versions = load_schema_versions()
        current_version = current_versions[config.name]["version"]
        
        # For indexer
        if config.name == 'indexer':
            new_version = 86#int(requests.get('https://grants-stack-indexer-v2.gitcoin.co/version').text)
        else:
            new_version = get_latest_schema_version(config.db_params)
            
        if new_version is None:
            raise ValueError(f"Could not determine schema version for {config.name}")
        
        # Update last checked time regardless of whether version changed
        current_versions[config.name]["last_checked"] = datetime.now().isoformat()
        
        
      # Skip if version hasn't changed
        if current_version == new_version:
            logger.info(f"Schema {config.name} already at version {new_version}, skipping update")
            save_schema_versions(current_versions)
            return None
            
        logger.info(f"Updating {config.name} schema from version {current_version} to {new_version}")
        
        schema_name = f'chain_data_{new_version}'
        
        # Perform the update
        drop_foreign_tables(config.tables_to_drop, config.schema, DB_PARAMS)
        import_foreign_schema(
            schema_name,
            config.tables_to_import,
            DB_PARAMS,
            config.server,
            config.schema
        )
        
        for table in config.tables_to_create:
            create_table_from_definition(
                table,
                schema_name,
                config.schema,
                config.server,
                DB_PARAMS
            )
        
        # Update version and last checked time
        current_versions[config.name]["version"] = new_version
        save_schema_versions(current_versions)
        
        logger.info(f"Schema update completed successfully for {config.name}")
        return new_version
        
    except Exception as e:
        logger.error(f"Schema update failed for {config.name}: {e}")
        raise

def main():
    """Main execution function."""
    try:
        updates = []
        
        # Update MACI schema
        maci_version = update_schema(MACI_CONFIG)
        if maci_version:
            updates.append(f"MACI to {maci_version}")
        
        # Update Indexer schema
        indexer_version = update_schema(INDEXER_CONFIG)
        if indexer_version:
            updates.append(f"Indexer to {indexer_version}")
            
        if updates:
            logger.info(f"Successfully updated schemas: {', '.join(updates)}")
        else:
            logger.info("No schema updates were necessary")
            
    except Exception as e:
        logger.error(f"Schema update failed: {e}")
        print("Schema update failed.")
        exit(1)

if __name__ == "__main__":
    main()