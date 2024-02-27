from datetime import datetime, timedelta
import pyarrow.parquet as pq
import pyarrow as pa
import io
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def transform_ratecode(local_path: str, ratecode_path: str) -> str:
    """Create the ratecode dim"""
    # download_file_name = ti.xcom_pull(task_ids='retrieve_from_src')
    table = pq.read_table(local_path)
    df_all = table.to_pandas()
    df = df_all[['RatecodeID']].copy()
    df['FinalRate'] = df['RatecodeID'].map({
        1.0: ' Standard rate',
        2.0: 'JFK',
        3.0: 'Newark',
        4.0: 'Nassau or Westchester',
        5.0: 'Negotiated fare',
        6.0: 'Group ride'
    })  

     # Handle non-finite values (NA or inf) before casting to integer
    df['RatecodeID'].fillna(0, inplace=True)
    df['RatecodeID'] = df['RatecodeID'].astype(int)

    df.to_parquet(ratecode_path, compression='gzip', index=False)  
    return ratecode_path

def load_ratecode(ratecode_path: str, key: str, bucket_name: str):    
    """Load to ratecode table"""
    hook = S3Hook('airflow_aws_s3_conn')
    hook.load_file(filename=ratecode_path, bucket_name=bucket_name, key=key,)

default_args = {
    'owner': 'marrie',
    'start_date': datetime(2023, 11, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'ratecode_dim',
    default_args=default_args,
    description='adding new columns to the data and pushing it to clean s3',
    schedule='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['rtc_dim']
)

months =  [1,2,3,4,5,6,7,8,9,10,11,12]

for month in months:
    data = f'yellow_tripdata_2020-{month:02}.pq.gz' 
    clean = f'ratecode_2020-{month:02}.parquet.gz' # change according to dim being created
    bucket_name = 'nytaxi-data-raw-us-east-airflow-dev-clensed'
    local_path = f'/home/devmarrie/airflow/data/cl/{data}' # clean data
    ratecode_path = f'/home/devmarrie/airflow/data/ratecode/{clean}' # create the vendors folder
    key =f'data/ratecode_dim/{clean}'

    task_transform_ratecode = PythonOperator(
        task_id=f'create_a_ratecode_{month}',
        python_callable=transform_ratecode,
        op_kwargs= {
            'local_path': local_path,
            'ratecode_path': ratecode_path
        },
        dag=dag
    )

    task_load_ratecode = PythonOperator(
        task_id=f'load_to_ratecode_table_{month}',
        python_callable=load_ratecode,
        op_kwargs= {
            'ratecode_path': ratecode_path,
            'key': key,
            'bucket_name': bucket_name
        },
        dag=dag
    )

    task_transform_ratecode >> task_load_ratecode



