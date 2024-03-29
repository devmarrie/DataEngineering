from datetime import datetime, timedelta
import pyarrow.parquet as pq
import pyarrow as pa
import io
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def s3_source(bucket_name:str, key:str, dest: str) -> str:
    """
    Read data form an s3 key
    Args:
        bucket_name (str): The name of the S3 bucket.
        key (str): The key of the file to read.
    """
    hook = S3Hook('airflow_aws_s3_conn')
    file_name = hook.download_file(key=key, bucket_name=bucket_name, local_path=dest, preserve_file_name=True, use_autogenerated_subdir=False)
    return file_name

def transform(dest_file, pay_path: str) -> str:
    """Create the payment dim"""
    # download_file_name = ti.xcom_pull(task_ids='retrieve_from_src')
    table = pq.read_table(dest_file)
    df_all = table.to_pandas()
    df = df_all[['payment_type']].copy()

    df['payment_method'] = df['payment_type'].map({
        1.0: 'credit card',
        2.0: 'cash',
        3.0: 'no charge',
        4.0: 'dispute',
        5.0: 'unknown',
        6.0: 'voided trip'
    })  

    # Handle non-finite values (NA or inf) before casting to integer
    df['payment_type'].fillna(0, inplace=True)
    df['payment_type'] = df['payment_type'].astype(int)
    
    df.to_parquet(pay_path, compression='gzip', index=False)  
    return pay_path

def load_to_clean(pay_path: str, clean_key: str, bucket_name: str):    
    """Load to payment table"""
    # filename = ti.xcom_pull(task_ids='sql_trans')
    hook = S3Hook('airflow_aws_s3_conn')
    hook.load_file(filename=pay_path, bucket_name=bucket_name, key=clean_key,)

default_args = {
    'owner': 'marrie',
    'start_date': datetime(2023, 11, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'payment_dim',
    default_args=default_args,
    description='adding new columns to the data and pushing it to clean s3',
    schedule='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['pay_dim']
)

months =  [1,2,3,4,5,6,7,8,9,10,11,12]

for month in months:
    data = f'yellow_tripdata_2020-{month:02}.pq.gz' 
    clean = f'payments_2020-{month:02}.parquet.gz' # change according to dim being created
    bucket_name = 'nytaxi-data-raw-us-east-airflow-dev-clensed'
    key = f'data/Trip_Fact/{data}' # dependant on the name of the fact
    dest = f'/home/devmarrie/airflow/data/cl/' # clean data
    dest_file = f'{dest}{data}'
    pay_path = f'/home/devmarrie/airflow/data/pay/{clean}' # create the pay path
    clean_key =f'data/payment_dim/{clean}'

    

    task_from_source = PythonOperator(
        task_id=f'retrieve_from_full_{month}',
        python_callable=s3_source,
        op_kwargs= {
            'bucket_name': bucket_name,
            'key': key,
            'dest': dest
        },
        dag=dag
    )


    task_to_pa_table = PythonOperator(
        task_id=f'modify_the_columns_{month}',
        python_callable=transform,
        op_kwargs= {
            'dest_file': dest_file,
            'pay_path': pay_path
        },
        dag=dag
    )

    task_load_clensed_data = PythonOperator(
        task_id=f'load_to_clean_{month}',
        python_callable=load_to_clean,
        op_kwargs= {
            'pay_path': pay_path,
            'clean_key': clean_key,
            'bucket_name': bucket_name
        },
        dag=dag
    )

    task_from_source >> task_to_pa_table >> task_load_clensed_data



