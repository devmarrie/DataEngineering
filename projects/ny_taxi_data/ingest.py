from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash  import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def load(local_path:str, key: str, bucket_name:str):
    """Load the data to our s3 bucket"""
    hook = S3Hook('airflow_aws_s3_conn')
    hook.load_file(filename=local_path, bucket_name=bucket_name, key=key)


default_args = {
    'owner': 'your_name',
    'start_date': datetime(2023, 11, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'data_model_sample',
    default_args=default_args,
    description='following the data model',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['star_schema']
)

year = 2020
month = 1
data_file = f'yellow_tripdata_{year}-{month:02}.csv.gz'
local_path = f'/home/devmarrie/airflow/data/csv/{data_file}'
url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/{data_file}'
bucket_name = 'nytaxi-data-raw-us-east-airflow-dev'
key = f'data/yellow/{data_file}'

task_read_from_source = BashOperator(
    task_id=f'read_from_source',
    bash_command=f'curl -sSL {url} -o {local_path} && echo "Download successful!" || echo "Download failed"',
    dag=dag
    )

task_load_to_s3 = PythonOperator(
    task_id='load_the_csv',
    python_callable=load,
    op_kwargs={
        'local_path': local_path,
        'key': key,
        'bucket_name': bucket_name
    },
    dag=dag
)

task_read_from_source >> task_load_to_s3