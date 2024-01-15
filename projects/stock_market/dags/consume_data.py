from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BIGQUERY_DATASET = os.environ.get("GCP_GCS_DATASET", 'tesla_stocks_dataset')

default_args = {
    'owner': 'stocks',
    'start_date': datetime(2024,1,1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# def create_spark_connection():

# def push_data_to_bq():
def json_read():
    with open("/.google/credentials/tesla-stocks-410911-c875b633dff5.json", "r") as key_file:
        key_file_content = key_file.read()
        print(f"Key File Content: {key_file_content}")

with DAG('consume_stock_data',
         default_args=default_args,
         schedule='@daily',
         catchup= False
        ) as dag:
    task_contents_read = PythonOperator(
        task_id="readable_data",
        python_callable=json_read
    )

    task_create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        project_id=PROJECT_ID,
        dataset_id=BIGQUERY_DATASET,
        table_id="test_table"
        )
    task_create_table