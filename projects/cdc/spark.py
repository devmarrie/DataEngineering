#!/usr/bin/env python

import pyspark

from pyspark.sql import  SparkSession
from google.cloud import bigquery

CREDENTIALS_FILE = 'credentials/tesla-stocks-410911-c875b633dff5.json'

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("test") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.24.2") \
    .config("spark.sql.warehouse.dir", "warehouse/configclear") \
    .getOrCreate()

# show it that the csv file has a header
df = spark.read \
    .option("header", "true") \
    .csv('taxi_zone_lookup.csv')


df.write.parquet('zones')

df_zones = spark .read\
           .parquet('zones/')

# Set up BigQuery client with service account authentication
client = bigquery.Client.from_service_account_json(CREDENTIALS_FILE)

# Specify the destination table (update project, dataset, and table names)
table_ref = client.dataset('tesla-stocks-410911.tesla_stocks_dataset').table('reports_yellow')

#an improvise to just push data to BQ
df_zones.write.format('bigquery') \
    .option('table', table_ref) \
    .save()

# Stop SparkSession
spark.stop()
