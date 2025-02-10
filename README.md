# Car Price Pipeline

This project demonstrates a simple ETL pipeline designed to automate the extraction, processing, and loading of car price data into a warehouse for analysis and comparison. 

**(Disclaimer: Data used for educational purpose)**

![tittle](https://carsomemy.s3.amazonaws.com/wp/used%20cars%20rs.jpg)

**Project aim:**
- Update on car information
- Aftermarket price sell by date

## Pipeline design 

![Pipeline diagram](img/pipeline_design.jpg)

## Tools
- Webscrape: Selenium
- Data Warehouse: PostgreSQL
- Language: Python, SQL
- Orchestration: Airflow
- Container: Docker

## Data Ingestion

#### Data Source (Extraction)

1st stage: Extracting data.

- CSV File
- WebScrape: Multiple source
- Tools: Selenium

    ```
    Webriver Chrome:
    - Chrome version (Version 132.0.6834.160 (Official Build) (64-bit))
    - Webdriver version (https://storage.googleapis.com/chrome-for-testing-public/132.0.6834.160/win64/chrome-win64.zip)
    ```

#### Data Clean (Transformation)

2nd stage: Cleaning, transform, and aggregating.

![Data Staging diagram](img/transformation_flow.jpg)

#### 3 layer Staging (Load)

Implementation of 3 data layer stage:
1. 1st layer: Load raw (raw.mudah, raw.carlist, raw.carsome)
2. 2nd layer: Load combined car info (car_data)
3. 3rd layer: Load data to mart (car_info & car_price_trend)

**Schema**

![Schema](img/schema_design.jpg)
