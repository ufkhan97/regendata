from dune_client.client import DuneClient
import pandas as pd
import logging
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

def main():
    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    # Load environment variables
    try:
        if load_dotenv():
            logger.info("Loaded .env file")
        else:
            logger.info("No .env file found or loaded")
    except ImportError:
        logger.info("dotenv not installed, skipping .env file loading")

    # Initialize Dune client and get query results
    DUNE_API_KEY = os.getenv('DUNE_API_KEY')
    logger.info("Initializing Dune client")
    dune = DuneClient(DUNE_API_KEY)
    logger.info("Fetching latest results from query 4118421")
    query_result = dune.get_latest_result(4118421)
    logger.info("Successfully retrieved query results")
    query_result_df = pd.DataFrame(query_result.result.rows)

    # Get database connection details
    DB_HOST = os.getenv('DB_HOST')
    DB_NAME = 'Grants'
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')

    # Create database connection
    db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
    engine = create_engine(db_url)

    # Upload dataframe to database
    table_name = 'allov2_distribution_events_for_leaderboard'
    query_result_df.to_sql(
        name=table_name,
        con=engine,
        schema='experimental_views',
        if_exists='replace',
        index=False
    )

    logger.info(f"Successfully uploaded {len(query_result_df)} rows to table {table_name}")

if __name__ == "__main__":
    main()
