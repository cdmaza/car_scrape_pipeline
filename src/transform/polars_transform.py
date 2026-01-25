import polars as pl
import psycopg2
import logging
import os
from datetime import datetime
from src.common.conn_details import POSTGRES_CONFIG
from src.common.web_source import WEB_SOURCES

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

DATA_DIR = "data/transformed"


def extract_from_postgres(table_name: str) -> pl.DataFrame:
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    query = f"SELECT id, raw_data, scraped_at FROM {table_name} WHERE processed = FALSE"

    try:
        df = pl.read_database(query, conn)
        logger.info(f"Extracted {len(df)} rows from {table_name}")
        return df
    finally:
        conn.close()


def transform_carlist(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return df

    transformed = df.with_columns([
        pl.col("raw_data").str.json_path_match("$.title").alias("title"),
        pl.col("raw_data").str.json_path_match("$.price").alias("price"),
        pl.col("raw_data").str.json_path_match("$.year").alias("year"),
        pl.col("raw_data").str.json_path_match("$.mileage").alias("mileage"),
        pl.col("raw_data").str.json_path_match("$.location").alias("location"),
        pl.col("raw_data").str.json_path_match("$.brand").alias("brand"),
        pl.col("raw_data").str.json_path_match("$.model").alias("model"),
        pl.lit("carlist").alias("source"),
        pl.col("scraped_at").alias("extracted_at"),
    ]).select([
        "id", "title", "price", "year", "mileage",
        "location", "brand", "model", "source", "extracted_at"
    ])

    return transformed


def transform_carsome(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return df

    transformed = df.with_columns([
        pl.col("raw_data").str.json_path_match("$.title").alias("title"),
        pl.col("raw_data").str.json_path_match("$.price").alias("price"),
        pl.col("raw_data").str.json_path_match("$.year").alias("year"),
        pl.col("raw_data").str.json_path_match("$.mileage").alias("mileage"),
        pl.col("raw_data").str.json_path_match("$.location").alias("location"),
        pl.col("raw_data").str.json_path_match("$.brand").alias("brand"),
        pl.col("raw_data").str.json_path_match("$.model").alias("model"),
        pl.lit("carsome").alias("source"),
        pl.col("scraped_at").alias("extracted_at"),
    ]).select([
        "id", "title", "price", "year", "mileage",
        "location", "brand", "model", "source", "extracted_at"
    ])

    return transformed


TRANSFORM_MAP = {
    "raw_carlist": transform_carlist,
    "raw_carsome": transform_carsome,
}


def save_to_parquet(df: pl.DataFrame, source_name: str):
    os.makedirs(DATA_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = f"{DATA_DIR}/{source_name}_{timestamp}.parquet"
    df.write_parquet(filepath)
    logger.info(f"Saved {len(df)} rows to {filepath}")
    return filepath


def mark_as_processed(table_name: str, ids: list):
    if not ids:
        return

    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()

    try:
        cursor.execute(
            f"UPDATE {table_name} SET processed = TRUE WHERE id = ANY(%s)",
            (ids,)
        )
        conn.commit()
        logger.info(f"Marked {len(ids)} rows as processed in {table_name}")
    finally:
        cursor.close()
        conn.close()


def load_to_staging(df: pl.DataFrame):
    if df.is_empty():
        return

    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()

    try:
        for row in df.iter_rows(named=True):
            cursor.execute(
                """
                INSERT INTO staging_cars
                (source_id, title, price, year, mileage, location, brand, model, source, extracted_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    row["id"], row["title"], row["price"], row["year"],
                    row["mileage"], row["location"], row["brand"],
                    row["model"], row["source"], row["extracted_at"]
                )
            )
        conn.commit()
        logger.info(f"Loaded {len(df)} rows to staging_cars")
    finally:
        cursor.close()
        conn.close()


def run_transform():
    logger.info("Starting Polars transformation")

    for source in WEB_SOURCES:
        table_name = source["table"]
        source_name = source["name"]

        logger.info(f"Processing {source_name}...")

        df = extract_from_postgres(table_name)
        if df.is_empty():
            logger.warning(f"No new data to process from {table_name}")
            continue

        transform_func = TRANSFORM_MAP.get(table_name)
        if not transform_func:
            logger.error(f"No transform function for {table_name}")
            continue

        transformed_df = transform_func(df)

        save_to_parquet(transformed_df, source_name)
        load_to_staging(transformed_df)
        mark_as_processed(table_name, df["id"].to_list())

    logger.info("Polars transformation completed")


if __name__ == "__main__":
    run_transform()
