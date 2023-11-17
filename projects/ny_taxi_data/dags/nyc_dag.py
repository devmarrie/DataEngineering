from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from ingest import fetch

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'nyc_taxi_data_dag',
    default_args=default_args,
    description='My first etl code'
)

run_etl = PythonOperator(
    task_id='complete_nyc_taxi_etl',
    python_callable=fetch,
    op_args=['https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-04.csv.gz'],
    dag=dag 
)

run_etl
