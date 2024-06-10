import logging
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import min, max, avg, stddev

# load environment variables
load_dotenv()

def avg_dis_to_s3(df, file_name, save_path, file_format="parquet", options={}):
    # confgure s3 env
    try:
        avg_discount = df.groupBy('Category').agg({"PercentageDiscount": "avg"}).withColumnRenamed("avg(PercentageDiscount)", "AverageDiscount")
        # avg_discount.write \
        #             .format(file_format) \
        #             .options(**options) \
        #             .save(f"{file_name}/{save_path}")
        avg_discount.write.parquet(f"{file_name}/{save_path}")
        logging.info(f'Data successfully written to s3 {file_name}/{save_path}')
    except Exception as e:
        logging.error(f'Failed to write data to S3: {e}')
        
# Which products have the highest discounts?
def highest_dis(df, file_name, save_path):
    try:
        highest_discounted_product = df.orderBy(col("PercentageDiscount").desc()).limit(10)
        highest_discounted_product.write.parquet(f"{file_name}/{save_path}")
        logging.info(f'Data successfully written to {file_name}/{save_path}')
    except Exception as e:
        logging.error(f'Failed to write data to S3: {e}')

# Which products have the lowest discounts?
def lowest_dis(df, file_name, save_path):
    try:
        lowest_discounted_product = df.orderBy(col("PercentageDiscount").asc()).limit(10)
        lowest_discounted_product.write.parquet(f"{file_name}/{save_path}")
        logging.info(f'Data successfully written to {file_name}/{save_path}')
    except Exception as e:
        logging.error(f'Failed to write data to S3: {e}')

# What is the distribution of product prices across categories?
def price_dist(df, file_name, save_path):
    try:
        price_distribution = df.groupBy("Category").agg(
            min("ApplicablePrice").alias("MinPrice"),
            max("ApplicablePrice").alias("MaxPrice"),
            avg("ApplicablePrice").alias("AvgPrice"),
            stddev("ApplicablePrice").alias("StddevPrice")
        )
        price_distribution.write.parquet(f"{file_name}/{save_path}")
        logging.info(f'Data successfully written to {file_name}/{save_path}')
    except Exception as e:
        logging.error(f'Failed to write data to S3: {e}')

# How many products have a discount applied per category? 
def dis_products(df, file_name, save_path):
    try:
        discounted_products_count = df.filter(col("PercentageDiscount") > 0) \
                                 .groupBy("Category") \
                                 .count()
        discounted_products_count.write.parquet(f"{file_name}/{save_path}")
        logging.info(f'Data successfully written to {file_name}/{save_path}')
    except Exception as e:
        logging.error(f'Failed to write data to S3: {e}')

# Most expensive products on Carrefour that day
def top_expensive(df, file_name, save_path):
    try:
        top_expensive_products = df.orderBy(col("ApplicablePrice").desc()).limit(10)
        top_expensive_products.write.parquet(f"{file_name}/{save_path}")
        logging.info(f'Data successfully written to {file_name}/{save_path}')
    except Exception as e:
        logging.error(f'Failed to write data to S3: {e}')

# 10 cheapest products on Carrefour that day
def top_cheap(df, file_name, save_path):
    try:
        top_cheap_products = df.orderBy(col("ApplicablePrice").asc()).limit(10)
        top_cheap_products.write.parquet(f"{file_name}/{save_path}")
        logging.info(f'Data successfully written to {file_name}/{save_path}')
    except Exception as e:
        logging.error(f'Failed to write data to S3: {e}')
                
if __name__ == '__main__':       
    spark = SparkSession.builder \
        .master('local') \
        .appName('c4') \
        .getOrCreate()
    
    df = spark.read.parquet('data/pq/all_foods')
    file_name = 'data/pq/analysis_n_storage'

    avg_dis_to_s3(df, file_name, 'avg-discount.parquet')
    highest_dis(df, file_name, 'highest_discounted_product.parquet')
    lowest_dis(df, file_name, 'lowest_discounted_product.parquet')
    price_dist(df, file_name, 'price_distribution.parquet')
    dis_products(df, file_name, 'discounted_products_count.parquet')
    top_expensive(df, file_name, 'top_expensive_products.parquet')
    top_cheap(df, file_name, 'top_cheap_products.parquet')
    spark.stop()


