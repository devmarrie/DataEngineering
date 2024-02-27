from datetime import datetime, timedelta
import pyarrow.parquet as pq
import pyarrow as pa
import io
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def transform_vendors(local_path: str, vendor_path: str) -> str:
    """Create the vendors dim"""
    # download_file_name = ti.xcom_pull(task_ids='retrieve_from_src')
    table = pq.read_table(local_path)
    df_all = table.to_pandas()
    df = df_all[['VendorID']].copy()
    df['VendorName'] = df['VendorID'].map({
        1.0: 'Creative Mobile Technologies',
        2.0: 'VeriFone Inc'
    })  
    
     # Handle non-finite values (NA or inf) before casting to integer
    df['VendorID'].fillna(0, inplace=True)
    df['VendorID'] = df['VendorID'].astype(int)

    df.to_parquet(vendor_path, compression='gzip', index=False)  
    return vendor_path

def load_vendors(vendor_path: str, key: str, bucket_name: str):    
    """Load to vendor table"""
    hook = S3Hook('airflow_aws_s3_conn')
    hook.load_file(filename=vendor_path, bucket_name=bucket_name, key=key,)

default_args = {
    'owner': 'marrie',
    'start_date': datetime(2023, 11, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'vendors_dim',
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
    clean = f'vendors_2020-{month:02}.parquet.gz' # change according to dim being created
    bucket_name = 'nytaxi-data-raw-us-east-airflow-dev-clensed'
    local_path = f'/home/devmarrie/airflow/data/cl/{data}' # clean data
    vendor_path = f'/home/devmarrie/airflow/data/vendors/{clean}' # create the vendors folder
    key =f'data/vendor_dim/{clean}'

    task_transform_vendors = PythonOperator(
        task_id=f'create_a_vendor_{month}',
        python_callable=transform_vendors,
        op_kwargs= {
            'local_path': local_path,
            'vendor_path': vendor_path
        },
        dag=dag
    )

    task_load_vendors = PythonOperator(
        task_id=f'load_to_vendors_table_{month}',
        python_callable=load_vendors,
        op_kwargs= {
            'vendor_path': vendor_path,
            'key': key,
            'bucket_name': bucket_name
        },
        dag=dag
    )

    task_transform_vendors >> task_load_vendors



