import os
import psycopg2
import logging
import threading
import time

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

def keepalive(conn):
    """Send a keepalive query every 60 seconds"""
    while True:
        try:
            with conn.cursor() as cur:
                cur.execute('SELECT 1;')
                logger.info('Keepalive query executed')
            time.sleep(60)  # Wait 60 seconds before next keepalive
        except:
            break

def long_running_query():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        
        # Start keepalive thread
        keepalive_thread = threading.Thread(target=keepalive, args=(conn,), daemon=True)
        keepalive_thread.start()
        
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