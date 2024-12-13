import os
import psycopg2
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database configuration
DB_PARAMS = {
    'host': os.environ['DB_HOST'],
    'port': os.environ['DB_PORT'],
    'dbname': 'Grants',
    'user': os.environ['DB_USER'],
    'password': os.environ['DB_PASSWORD'],
    'connect_timeout': 600,  # 10 minutes
}

def long_running_query():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        with conn.cursor() as cursor:
            logger.info('Starting long-running query (SELECT pg_sleep(600);)')
            cursor.execute('SELECT pg_sleep(600);')
            logger.info('Long-running query completed successfully.')
    except psycopg2.Error as e:
        logger.error(f'Database error: {e}')
        raise
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == '__main__':
    long_running_query()