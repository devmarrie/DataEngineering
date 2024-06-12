import logging
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import min, max, avg, stddev

# Load environment variables
load_dotenv()

def avg_dis_to_s3(df, file_name, save_path):
    try:
        avg_discount = df.groupBy('Category').agg({"PercentageDiscount": "avg"}).withColumnRenamed("avg(PercentageDiscount)", "AverageDiscount")
        avg_discount.coalesce(1).write.option("header", "true").csv(f"{file_name}/{save_path}")
        logging.info(f'Data successfully written to {file_name}/{save_path}')
    except Exception as e:
        logging.error(f'Failed to write data to S3: {e}')
        
def highest_dis(df, file_name, save_path):
    try:
        highest_discounted_product = df.orderBy(col("PercentageDiscount").desc()).limit(10)
        highest_discounted_product.coalesce(1).write.option("header", "true").csv(f"{file_name}/{save_path}")
        logging.info(f'Data successfully written to {file_name}/{save_path}')
    except Exception as e:
        logging.error(f'Failed to write data to S3: {e}')

def lowest_dis(df, file_name, save_path):
    try:
        lowest_discounted_product = df.orderBy(col("PercentageDiscount").asc()).limit(10)
        lowest_discounted_product.coalesce(1).write.option("header", "true").csv(f"{file_name}/{save_path}")
        logging.info(f'Data successfully written to {file_name}/{save_path}')
    except Exception as e:
        logging.error(f'Failed to write data to S3: {e}')

def price_dist(df, file_name, save_path):
    try:
        price_distribution = df.groupBy("Category").agg(
            min("ApplicablePrice").alias("MinPrice"),
            max("ApplicablePrice").alias("MaxPrice"),
            avg("ApplicablePrice").alias("AvgPrice"),
            stddev("ApplicablePrice").alias("StddevPrice")
        )
        price_distribution.coalesce(1).write.option("header", "true").csv(f"{file_name}/{save_path}")
        logging.info(f'Data successfully written to {file_name}/{save_path}')
    except Exception as e:
        logging.error(f'Failed to write data to S3: {e}')

def dis_products(df, file_name, save_path):
    try:
        discounted_products_count = df.filter(col("PercentageDiscount") > 0) \
                                 .groupBy("Category") \
                                 .count()
        discounted_products_count.coalesce(1).write.option("header", "true").csv(f"{file_name}/{save_path}")
        logging.info(f'Data successfully written to {file_name}/{save_path}')
    except Exception as e:
        logging.error(f'Failed to write data to S3: {e}')

def top_expensive(df, file_name, save_path):
    try:
        top_expensive_products = df.orderBy(col("ApplicablePrice").desc()).limit(10)
        top_expensive_products.coalesce(1).write.option("header", "true").csv(f"{file_name}/{save_path}")
        logging.info(f'Data successfully written to {file_name}/{save_path}')
    except Exception as e:
        logging.error(f'Failed to write data to S3: {e}')

def top_cheap(df, file_name, save_path):
    try:
        top_cheap_products = df.orderBy(col("ApplicablePrice").asc()).limit(10)
        top_cheap_products.coalesce(1).write.option("header", "true").csv(f"{file_name}/{save_path}")
        logging.info(f'Data successfully written to {file_name}/{save_path}')
    except Exception as e:
        logging.error(f'Failed to write data to S3: {e}')
                
if __name__ == '__main__':       
    spark = SparkSession.builder \
        .master('local') \
        .appName('c4') \
        .getOrCreate()
    
    df = spark.read.parquet('data/pq/all_foods')
    file_name = 'data/csv/analysed_data'

    avg_dis_to_s3(df, file_name, 'avg-discount')
    highest_dis(df, file_name, 'highest_discounted_product')
    lowest_dis(df, file_name, 'lowest_discounted_product')
    price_dist(df, file_name, 'price_distribution')
    dis_products(df, file_name, 'discounted_products_count')
    top_expensive(df, file_name, 'top_expensive_products')
    top_cheap(df, file_name, 'top_cheap_products')
    spark.stop()
