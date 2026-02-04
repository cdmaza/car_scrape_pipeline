import importlib
import psycopg2
import logging
import json
from datetime import datetime
import os
from src.common.conn_details import POSTGRES_CONFIG
from src.common.web_source import WEB_SOURCES

VALID_TABLES = {source["table"] for source in WEB_SOURCES}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"logs/extract/extract_{datetime.now().strftime('%Y-%m-%d')}.log")
    ]
)
logger = logging.getLogger(__name__)


def load_client(module_name: str, class_name: str):
    try:
        module = importlib.import_module(module_name)
        client_class = getattr(module, class_name)
        return client_class()
    except (ImportError, AttributeError) as e:
        logger.error(f"Failed to load {module_name}.{class_name}: {e}")
        return None


def run_extraction():
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()

    try:
        for source in WEB_SOURCES:
            client = load_client(source["module"], source["class"])
            if not client:
                continue

            logger.info(f"Scraping {source['name']}...")
            data = client.scrape_site()

            if not data:
                logger.warning(f"No data from {source['name']}")
                continue

            table_name = source["table"]
            if table_name not in VALID_TABLES:
                logger.error(f"Invalid table name: {table_name}")
                continue

            for row in data:
                cursor.execute(
                    f"INSERT INTO {table_name} (raw_data, scraped_at) VALUES (%s, %s)",
                    (json.dumps(row), datetime.now())
                )

            logger.info(f"Inserted {len(data)} rows into {table_name}")

        conn.commit()
        logger.info("Extraction completed successfully")
    except Exception as e:
        conn.rollback()
        logger.error(f"Extraction failed: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def setup_logging():
    os.makedirs("logs/extract", exist_ok=True)

def main():
    setup_logging()
    run_extraction()

if __name__ == "__main__":
    main()
