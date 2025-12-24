import os
import glob
import pandas as pd
from datetime import datetime, timedelta 

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


#get data
def get_csv():
    dataframes = []
    file_path = "c:/Users/User/Desktop/Project/data_project/car_scrape_pipeline/dags/etl/data"

    csv_files = glob.glob(os.path.join(file_path, "*.csv"))

    try:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File {file_path} does not exist.")
        
        for file in csv_files:
            df = pd.read_csv(file)  
            dataframes.append(df)   

        combined_df = pd.concat(dataframes, ignore_index=True)

        return combined_df
    
    except FileNotFoundError as e:
        print(e)
    except Exception as e:
        print(f"An error occurred: {e}")

#transform
def transform(**kwargs):

    combined_df = get_csv()
    combined_df = combined_df.dropna()

    #format
    #data types & case sensitive
    # "year_produce", "brand", "model","car_type", "gear_type", "engine_cap","mileage","year_extracted", "old_price"
    combined_df["year_produce"] = combined_df["year_produce"].astype(str)
    combined_df["brand"] = combined_df["brand"].astype(str)
    combined_df["model"] = combined_df["model"].astype(str)
    combined_df["car_type"] = combined_df["car_type"].astype(str)
    combined_df["gear_type"] = combined_df["gear_type"].astype(str)
    combined_df["engine_cap"] = combined_df["engine_cap"].astype(str)
    combined_df["mileage"] = combined_df["mileage"].astype(int)
    combined_df["year_extracted"] = combined_df["year_extracted"].astype(str)
    combined_df["old_price"] = combined_df["old_price"].astype(int)


    string = 'str'
    for col in combined_df.columns:
        if "engine_cap" in col:
            string = 'float'
        elif string is not 'float':
            combined_df[col] = combined_df[col].astype(string).title()

        combined_df[col] = combined_df[col].astype(string)

    #all lower case
    combined_df = combined_df.applymap(lambda x: x.lower() if isinstance(x, str) else x)


    #create primary_key for table
    #table car
    combined_df['temp_engine_cap'] = combined_df['engine_cap'].str.replace(".", "")
    combined_df['car_id'] = combined_df.apply(lambda row: f"{row['brand']}{row['temp_engine_cap']}{row['year_produce']}", axis=1)
    #table price_trend
    combined_df['price_id'] = combined_df.apply(lambda row: f"{row['car_id']}{row['year_extracted']}", axis=1)

    combined_df = combined_df.drop_duplicates()
    kwargs['ti'].xcom_push(key='combined_df', value=combined_df.to_dict())


#load
def csv_to_postgres(**kwargs):
    combined_df = pd.DataFrame(kwargs['ti'].xcom_pull(key='combined_df'))

    hook = PostgresHook(postgres_conn_id="postgres_localhost")
    conn = hook.get_conn()
    cursor = conn.cursor()

    car_columns = ['car_id', 'brand', 'model', 'car_type', 'gear_type', 'engine_cap', 'year_produce']
    car_df = combined_df[car_columns]
    for index, row in car_df.iterrows():
        cursor.execute("""
            INSERT INTO car (car_id, brand, model, car_type, gear_type, engine_cap, year_produce)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (car_id) DO NOTHING
        """, tuple(row))

    # Insert data into price_trend table
    price_trend_columns = ['price_id', 'car_id', 'year_extracted', 'mileage', 'old_price']
    price_trend_df = combined_df[price_trend_columns]
    for index, row in price_trend_df.iterrows():
        cursor.execute("""
            INSERT INTO price_trend (price_id, car_id, year_extracted, mileage, price)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (price_id) DO NOTHING
        """, tuple(row))

    conn.commit()
    cursor.close()
    conn.close()

default_args = {
    'owner': 'cdmaza',
    'retries': 5,
    'retry_delay': timedelta(minutes=10)
}

with DAG(
    dag_id="car_scrape_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 19),
    schedule_interval='@weekly'
) as dag:
    
    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id='csv_to_postgres',
        python_callable=csv_to_postgres,
        provide_context=True
    )

    transform_task >> load_task