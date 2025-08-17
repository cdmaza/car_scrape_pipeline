import logging

from extract.extract_carlist import extract_carlist
from extract.extract_carsome import extract_carsome

def get_carlist():
    carlist_data = extract_carlist()
    return carlist_data

def get_carsome():
    carsome_data = extract_carsome()
    return carsome_data


def quaility_check():

    schema conformity: Are the fields/types as expected?

    Data availability: Was the data source reachable? Any timeouts?

    Record count anomaly: Compare record counts to historical averages.

    Timestamp freshness: Is the data current or stale?

    Null/missing values: Are any required fields missing?

    Duplicate records: Are there repeated rows when there shouldn't be?

    Source identifier checks: Verify integrity of IDs, keys, etc.

def log_extract():
    # Basic configuration
    logging.basicConfig(level=logging.INFO)

    # Example logs
    logging.debug("Debug message (only visible if level=DEBUG)")
    logging.info("Informational message")
    logging.warning("Warning!")
    logging.error("An error occurred")
    logging.critical("Critical failure")