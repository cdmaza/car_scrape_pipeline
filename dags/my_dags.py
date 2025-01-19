import csv
import os
import glob
import logging
import pandas as pd
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

#get data
def get_csv():
    dataframes = []
    file_path = "c:/Users/User/Desktop/Project/data_project/car_scrape_pipeline/dags/etl/data"

    csv_files = glob.glob(os.path.join(file_path, "*.csv"))
    old_files = [file for file in csv_files if "old" in os.path.basename(file).lower()]
    new_files = [file for file in csv_files if "new" in os.path.basename(file).lower()]

    try:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File {file_path} does not exist.")
        
        for file in old_files:
            df = pd.read_csv(file)  
            dataframes.append(df)   
        
        old_combined_df = pd.concat(dataframes, ignore_index=True)
        
        for file in new_files:
            df = pd.read_csv(file)  
            dataframes.append(df) 

        new_combined_df = pd.concat(dataframes, ignore_index=True)

    except FileNotFoundError as e:
        print(e)
    except Exception as e:
        print(f"An error occurred: {e}")

#transform
def transform():

    #drop duplicate
    old_combined_df = old_combined_df.drop_duplicates()
    new_combined_df = new_combined_df.drop_duplicates()

    #missing value
    old_combined_df = old_combined_df.dropna()
    new_combined_df = new_combined_df.dropna()

    #format
    #data types
    string = 'str'
    for col in old_combined_df.columns:
        if "engine_cap" in col:
            string = 'float'
        elif string is not 'float':
            old_combined_df[col] = old_combined_df[col].astype(string).title()

        old_combined_df[col] = old_combined_df[col].astype(string)

    string = 'str'
    for col in new_combined_df.columns:
        if "engine_cap" in col:
            string = 'float'
        elif string is not 'float':
            new_combined_df[col] = new_combined_df[col].astype(string).title()

        new_combined_df[col] = new_combined_df[col].astype(string)  


def aggregrate():

    


#load
def csv_to_postgres(ds_nodash, next_ds_nodash):
    # step 1: connection to postgres
    hook = PostgresHook(postgres_conn_id="postgres_localhost")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("select * from orders where date >= %s and date < %s",
                   (ds_nodash, next_ds_nodash))
    with NamedTemporaryFile(mode='w', suffix=f"{ds_nodash}") as f:
    # with open(f"dags/get_orders_{ds_nodash}.txt", "w") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerows(cursor)
        f.flush()
        cursor.close()
        conn.close()
        logging.info("Saved orders data in text file: %s", f"dags/get_orders_{ds_nodash}.txt")
    # step 2: upload text file into S3
        s3_hook = S3Hook(aws_conn_id="minio_conn")
        s3_hook.load_file(
            filename=f.name,
            key=f"orders/{ds_nodash}.txt",
            bucket_name="airflow",
            replace=True
        )
        logging.info("Orders file %s has been pushed to S3!", f.name)

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
    
    task1 = PythonOperator(
        task_id="get_csv",
        python_callable=get_csv
    )

    task2 = PythonOperator(
        task_id="get_csv",
        python_callable=get_csv
    )

    task3 = PythonOperator(
        task_id="get_csv",
        python_callable=get_csv
    )

    task1 >> task2 >> task3