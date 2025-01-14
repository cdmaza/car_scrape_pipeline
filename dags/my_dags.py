from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime, timedelta
from random import randint

default_args ={
    'owner': 'coder2',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='test',
    default_args=default_args,
    start_date=datetime(2025, 1 ,14), 
    schedule_interval='0 0 * * *'
) as dag:

    task1 = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            create table if not exists dag_runs (
                dt date, 
                dag_id character varying,
                primary key (dt, dag_id)
            )
        """
    )

    task1