#!/usr/bin/env python

import os
import pyspark
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import types

# Dir containing the csv files 
data_dir = 'data'

# Initialize Spark Session
spark = SparkSession.builder \
        .master('local') \
        .appName('c4') \
        .getOrCreate()

# Define the schema to use
schema = types.StructType([
            types.StructField('ProductName', types.StringType(), True), 
            types.StructField('OriginalPrice', types.LongType(), True), 
            types.StructField('ApplicablePrice', types.LongType(), True), 
            types.StructField('Type', types.StringType(), True), 
            types.StructField('PercentageDiscount', types.LongType(), True), 
            types.StructField('Category', types.StringType(), True)
        ])

# Iterate over the folder while performing the schema change
for filename in os.listdir(data_dir):
    if filename.endswith('.csv'):
        # Get its full path
        file_path = os.path.join(data_dir, filename)

        # Read the data with the schema
        df = spark.read \
               .option('header', 'true') \
               .schema(schema) \
               .csv(file_path)

        # write to a parquet file mode append to include all
        df.write.mode('append').parquet('data/pq/all_foods')

print("Schema change successful now closing")

# Stop Spark session
spark.stop()




