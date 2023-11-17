import logging
import  os 
from pathlib import Path
from airflow import DAG
from datetime import datetime, timedelta
import pyarrow.csv as pv
import pyarrow.parquet as pq
from airflow.operators.python import PythonOperator
from airflow.operators.bash  import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator


data_file = 'yellow_tripdata_2021-07.csv.gz'
url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/{data_file}'
filename = f'data/{data_file}'
parquet_file = data_file.replace('.csv', '.parquet')
bucket_name = 'nytaxi-data-raw-us-east-airflow-dev'

def read_from_source(filename: str):
    """Convert the csv file to pq"""
    if not filename.endswith('csv.gz'):
        logging.error("Can only accept csv format at the momment")
    table = pv.read_csv(filename)
    pq.write_table(table, filename.replace('.csv', '.parquet'))

def load_to_s3(filename: str, key: str, bucket_name:str) -> None:
    """Load the data to our s3 bucket"""
    hook = S3Hook('airflow_aws_s3_conn')
    hook.load_file(filename=filename, bucket_name=bucket_name, key=key)

default_args = {
    'owner': 'your_name',
    'start_date': datetime(2023, 11, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    's3_airflow_conn',
    default_args=default_args,
    description='s3 airflow connection trial',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['nyc-de']
)

# load_operator = LocalFilesystemToS3Operator(
#     task_id='load to s3',
#     filename='data/yellow_tripdata_2021-05.csv.gz',
#     dest_bucket='nytaxi-data-raw-us-east-airflow-dev',
#     dest_key='data/yellow_tripdata_2021-05.csv.gz',
#     aws_conn_id='airflow_aws_s3_conn'
# )

task_read_from_source = BashOperator(
    task_id='read_from_source',
    bash_command=f'curl -sSL {url} -o {filename}',
    dag=dag
)

task_to_pq = PythonOperator(
    task_id='convert_csv_to_pq',
    python_callable=read_from_source,
    op_args=[filename],
    dag=dag
)

task_upload_to_s3 = PythonOperator(
    task_id='upload_to_s3',
    python_callable=load_to_s3,
    op_kwargs= {
        'filename': filename,
        'key': filename,
        'bucket_name': bucket_name
    },
    dag=dag
)

task_read_from_source >> task_to_pq >> task_upload_to_s3