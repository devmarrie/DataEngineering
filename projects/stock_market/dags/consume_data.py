from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType, IntegerType
import logging
import os

PROJECT_ID = os.environ.get('PROJECT_ID')

# BigQuery table details
bigquery_dataset = "tesla_stocks_dataset"
bigquery_table = "twenty_twenty_four_stocks"

def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.0," 
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")  # Set appropriate logging level
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn

# How do I know its a valid dataframe picked from the topic
def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'daily_stock_prices') \
            .option('startingOffsets', 'earliest') \
            .load() # this generates key value pairs
        logging.info("kafka dataframe created successfully")

    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")                                                                                                                                                            
    return spark_df

# Perform transformations
def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("Date", DateType(), False),
        StructField("Open", DoubleType(), False),
        StructField("High", DoubleType(), False),
        StructField("Low", DoubleType(), False),
        StructField("Close", DoubleType(), False),
        StructField("Adj Close", DoubleType(), False),
        StructField("Volume", IntegerType(), False),
    ])

    json_df = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    
    return json_df

def print_to_console(df):
    # Print to console:
    try:
        df.writeStream \
            .format('console') \
            .trigger(processingTime='3 seconds') \
            .option('numRows', 20) \
            .start().awaitTermination()
        logging.info("Data from Kafka stream is being printed to the console.")
    except Exception as e:
        logging.warning("Failed to print to console")

def push_to_bigquery(df):
    try:
        streaming_query = df.writeStream \
            .format("bigquery") \
            .option("project", PROJECT_ID) \
            .option("dataset", bigquery_dataset) \
            .option("table", bigquery_table) \
            .option("checkpointLocation", " /tmp/checkpoint") \
            .outputMode("append") \
            .trigger(processingTime="60 seconds") \
            .start()
        
        streaming_query.awaitTermination()
        logging.info("Streaming to BigQuery started successfully")
    except Exception as e:
        logging.warning(f"Failed to start streaming to BigQuery: {e}")

def write_to_bigquery():
    spark_conn = create_spark_connection()
    if spark_conn is not None:
        # Connect to Kafka and get the dataframe
        df = connect_to_kafka(spark_conn)
        json_df = create_selection_df_from_kafka(df)
        # print_to_console(json_df)
        
        push_to_bigquery(json_df)

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
    task_create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        project_id=PROJECT_ID,
        dataset_id=bigquery_dataset,
        table_id=bigquery_table
    )
    task_to_bq = PythonOperator(
        task_id="consume_to_bq",
        python_callable=write_to_bigquery
    )

task_create_table >> task_to_bq


# sink = cleaned_data.writeStream \
#                  .format("bigquery") \
#                  .option("project", "my-project-id") \
#                  .option("dataset", "my_dataset") \
#                  .option("table", "my_table") \
#                  .option("checkpointLocation", "/tmp/checkpoint") \
#                  .start()

# # Wait for termination
# sink.awaitTermination()