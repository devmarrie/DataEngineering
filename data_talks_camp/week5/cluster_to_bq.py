#!/usr/bin/env python
# coding: utf-8

# import findspark

# findspark.init()


import pyspark

pyspark.__version__

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import argparse


parser = argparse.ArgumentParser()

parser.add_argument('--input_green', required=True)
parser.add_argument('--input_yellow', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

input_green = args.input_green
input_yellow = args.input_yellow
output = args.output


spark = SparkSession.builder\
        .appName('test')\
        .getOrCreate()

# config the temporary bucket
spark.conf.set("temporaryGcsBucket", "dataproc-temp-europe-west6-253231033131-abiegpln")


df_green = spark.read.parquet(input_green)

df_green.show()

df_green.printSchema()

df_yellow = spark.read.parquet(input_yellow)

df_yellow.printSchema()


df_green.columns

df_yellow.columns



df_yellow = df_yellow \
    .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')


df_green = df_green \
    .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')


set(df_green.columns) & set(df_yellow.columns)



common_cols = [
    'VendorID',
    'pickup_datetime',
    'dropoff_datetime',
    'store_and_fwd_flag',
    'RatecodeID',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'fare_amount',
    'extra',
    'mta_tax',
    'tip_amount',
    'tolls_amount',
    'improvement_surcharge',
    'total_amount',
    'payment_type',
    'congestion_surcharge'
]



# we can now select te columns
df_green.select(common_cols).show()


# use the lit function to create a new column literal
df_green_serv = df_green \
    .select(common_cols) \
    .withColumn('service_type', F.lit('green'))


df_yellow_serv = df_yellow \
    .select(common_cols) \
    .withColumn('service_type', F.lit('yellow'))


# combine the two dataframes into one 
df_trips_data = df_green_serv.unionAll(df_yellow_serv)


# Now we can count the whole data per service type
df_trips_data.groupBy('service_type').count().show()


# Regiter the dataframe as a temporary table 
# so that it can be able to be called by spark as a table in a select statement
# registerTempTable() depricated
df_trips_data.createOrReplaceTempView('trips_data')


# Now run sql queries
spark.sql("""
SELECT service_type,
       count(1)
FROM trips_data
GROUP BY service_type
""").show()


# Revenue per month
df_result = spark.sql("""
SELECT 
    -- Reveneue grouping 
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month, 
    service_type, 

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

    -- Additional calculations
    AVG(passenger_count) AS avg_montly_passenger_count,
    AVG(trip_distance) AS avg_montly_trip_distance
FROM
    trips_data
GROUP BY
    1, 2, 3
""")



df_result.show()


df_result.write.format("bigquery") \
    .option("table",output) \
    .save()






