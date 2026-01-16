import logging
import sys

def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Format: Timestamp | Level | Component | Message
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(name)s] - %(message)s'
    )

    # Stream to console (Airflow captures this)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    return logger