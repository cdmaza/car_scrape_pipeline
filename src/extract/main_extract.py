from carlist_client import CarlistWebClient
from carsome_client import CarsomeWebClient
import psycopg2
import logging

logger = logging.getLogger(__name__)

# Config: maps each client class to its URL and target table
WEB_SOURCES = [
    {
        "client": CarlistWebClient,
        "url":
        "https://www.carlist.my/used-cars-for-sale/malaysia?sort=modification_date_search.desc",
        "table": "raw.raw_carlist"
    },
    {
        "client": CarsomeWebClient,
        "url": "https://www.carsome.com/used-cars",
        "table": "raw.raw_carsome"
    },
]

def run_extraction():
    conn_details = "host=postgres dbname=warehouse user=admin password=password"
    conn = psycopg2.connect(conn_details)
    cursor = conn.cursor()

    try:
        for source in WEB_SOURCES:
            client = source["client"]()
            table = source["table"]

            logger.info(f"Scraping {source['url']}...")
            html = client.fetch_html(source["url"])
            data = client.parse_html(html)

            if not data:
                logger.warning(f"No data from {table}")
                continue

            # Insert each row into the correct table
            for row in data:
                cursor.execute(
                    f"INSERT INTO {table} (product_name, price_string, scraped_at) VALUES (%s, %s, %s)",
                    (row["product_name"], row["price_string"], row["scraped_at"])
                )

            logger.info(f"Inserted {len(data)} rows into {table}")

        conn.commit()
        logger.info("All extractions completed.")

    except Exception as e:
        conn.rollback()
        logger.error(f"Extraction failed: {e}")
        raise
    finally:
        cursor.close()
        conn.close()