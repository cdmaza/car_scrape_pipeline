# Car Price Pipeline

This project demonstrates a simple ETL pipeline designed to automate the extraction, processing, and loading of car price data into a warehouse for analysis and comparison. 

**(Disclaimer: Data used for educational purpose)**

![tittle](https://carsomemy.s3.amazonaws.com/wp/used%20cars%20rs.jpg)

**Project aim:**
- Update on car information
- Aftermarket price sell by date

## Pipeline design 

![Pipeline diagram](docs\pipeline_design.jpg)

## Tools
- Source: Web Scrape
- Destination: PostgreSQL Warehouse
- Language: Python, SQL
- Code Testing: PyTest
- Data Quality: Cuallee
- Scheduler: Airflow
- CI/CD: Github Action

## Data Ingestion

#### Data Source (Extraction)

1st stage: Extracting data.

- CSV File
- Web Scrape: Taken from trusted source **(Data used for educational purpose)**

    ```
    Webriver Chrome:
    - Chrome version (Version 132.0.6834.160 (Official Build) (64-bit))
    - Webdriver version (https://storage.googleapis.com/chrome-for-testing-public/132.0.6834.160/win64/chrome-win64.zip)
    ```

#### Data Clean (Transformation)

2nd stage: Cleaning, transform, and aggregating.


Raw attribute name been extracted:
```
    title string
    brand string
    model string
    model_group string
    variant string
    body_type string
    transmission string
    mileage string
    type string
    capacity string
    price string
    manufactured string
    data_posted string
    date_extracted string
    detail_link string
```

**Attribute Schema**

![Data Staging diagram](docs\transformation_flow.jpg)

#### 3 layer Staging (Load)

Implementation of 3 data layer stage:
1. 1st layer: Load raw (mudah, carlist, carsome)
2. 2nd layer: Load combined car info (car_data)
3. 3rd layer: Load data to mart (car_info & car_price_trend)

**Database Schema**

![Schema](docs\schema_design.jpg)
