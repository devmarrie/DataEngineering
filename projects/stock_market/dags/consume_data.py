from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import logging
import os

PROJECT_ID = os.environ.get('PROJECT_ID')

# BigQuery table details
bigquery_dataset = "tesla_stocks_dataset"
bigquery_table = "twenty_twenty_four_stocks"

def create_spark_connection(s_conn):

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.google.cloud.spark:spark-bigquery-with-dependencies_2.13:0.36.0," 
                    "org.apache.spark:spark-streaming_2.13:3.5.0") \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn

# def create_spark_connection(bigquery_dataset, bigquery_table):
#     s_conn = None

#     try:
#         s_conn = SparkSession.builder \
#             .appName('SparkDataStreaming') \
#             .config('spark.driver.memory', '8g') \
#             .config('spark.executor.memory', '4g')  \
#             .config('spark.sql.warehouse.dir', '/tmp/spark-warehouse') \
#             .config('bigquery.write.dataset', bigquery_dataset) \
#             .config('bigquery.write.table', bigquery_table) \
#             .getOrCreate()

#         s_conn.sparkContext.setLogLevel("ERROR")
#         logging.info("Spark connection created successfully!")
#     except Exception as e:
#         logging.error(f"Couldn't create the spark session due to exception {e}")

#     return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'daily_stock_prices') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df

def write_to_bigquery_one(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids='connect_to_kafka')
    try:
        df.write \
        .outputMode("append") \
        .format("bigquery") \
        .option("table", f"{PROJECT_ID}:{bigquery_dataset}.{bigquery_table}") \
        .start()
        logging.info("Streaming to BigQuery started successfully")
    except Exception as e:
        logging.warning(f"Failed to start streaming to BigQuery: {e}")

def write_to_bigquery():
    s_conn = None
    spark_conn = create_spark_connection(s_conn)
    df = connect_to_kafka(spark_conn)
    return df

default_args = {
    'owner': 'stocks',
    'start_date': datetime(2024,1,1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('consume_stock_data',
         default_args=default_args,
         schedule='@daily',
         catchup= False
        ) as dag:
    task_create_spark_conn = PythonOperator(
        task_id="create_spark_connection",
        python_callable=write_to_bigquery
    )

    task_connect_to_kafka = PythonOperator(
        task_id="connect_to_kafka",
        python_callable=connect_to_kafka,
        provide_context=True
    )

    task_create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        project_id=PROJECT_ID,
        dataset_id=bigquery_dataset,
        table_id=bigquery_table
    )
    task_write_to_bigquery = PythonOperator(
        task_id="write_to_bigquery",
        python_callable=write_to_bigquery,
        provide_context=True
    )

task_create_table >> task_create_spark_conn >> task_connect_to_kafka >> task_write_to_bigquery