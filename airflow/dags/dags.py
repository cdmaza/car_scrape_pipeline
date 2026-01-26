from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "car_scrape_pipeline",
    default_args=default_args,
    description="Extract car data, transform with Polars, then dbt",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["car_scrape", "etl"],
)


def run_extraction():
    from src.extract.main_extract import run_extraction as extract
    extract()


def run_polars_transform():
    from src.transform.polars_transform import run_transform
    run_transform()


extract_task = PythonOperator(
    task_id="extract_car_data",
    python_callable=run_extraction,
    dag=dag,
)

polars_transform_task = PythonOperator(
    task_id="polars_transform",
    python_callable=run_polars_transform,
    dag=dag,
)

dbt_run_task = BashOperator(
    task_id="dbt_run",
    bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir /opt/airflow/dbt",
    dag=dag,
)

dbt_test_task = BashOperator(
    task_id="dbt_test",
    bash_command="cd /opt/airflow/dbt && dbt test --profiles-dir /opt/airflow/dbt",
    dag=dag,
)

extract_task >> polars_transform_task >> dbt_run_task >> dbt_test_task