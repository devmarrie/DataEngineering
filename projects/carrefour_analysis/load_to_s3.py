import logging
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import min, max, avg, stddev

# load environment variables
load_dotenv()

def avg_dis_to_s3(df, bucket_name, save_path, file_format="parquet", options={}):
    # confgure s3 env
    try:
        avg_discount = df.groupBy('Category').agg({"PercentageDiscount": "avg"}).withColumnRenamed("avg(PercentageDiscount)", "AverageDiscount")
        # avg_discount.write \
        #             .format(file_format) \
        #             .options(**options) \
        #             .save(f"s3a://{bucket_name}/{save_path}")
        avg_discount.write.parquet(f"s3a://{bucket_name}/{save_path}")
        logging.info(f'Data successfully written to s3a://{bucket_name}/{save_path}')
    except Exception as e:
        logging.error(f'Failed to write data to S3: {e}')
        
# Which products have the highest discounts?
def highest_dis(df, bucket_name, save_path):
    try:
        highest_discounted_product = df.orderBy(col("PercentageDiscount").desc()).limit(10)
        highest_discounted_product.write.parquet(f"s3a://{bucket_name}/{save_path}")
        logging.info(f'Data successfully written to s3a://{bucket_name}/{save_path}')
    except Exception as e:
        logging.error(f'Failed to write data to S3: {e}')

# Which products have the lowest discounts?
def lowest_dis(df, bucket_name, save_path):
    try:
        lowest_discounted_product = df.orderBy(col("PercentageDiscount").asc()).limit(10)
        lowest_discounted_product.write.parquet(f"s3a://{bucket_name}/{save_path}")
        logging.info(f'Data successfully written to s3a://{bucket_name}/{save_path}')
    except Exception as e:
        logging.error(f'Failed to write data to S3: {e}')

# What is the distribution of product prices across categories?
def price_dist(df, bucket_name, save_path):
    try:
        price_distribution = df.groupBy("Category").agg(
            min("ApplicablePrice").alias("MinPrice"),
            max("ApplicablePrice").alias("MaxPrice"),
            avg("ApplicablePrice").alias("AvgPrice"),
            stddev("ApplicablePrice").alias("StddevPrice")
        )
        price_distribution.write.parquet(f"s3a://{bucket_name}/{save_path}")
        logging.info(f'Data successfully written to s3a://{bucket_name}/{save_path}')
    except Exception as e:
        logging.error(f'Failed to write data to S3: {e}')

# How many products have a discount applied per category? 
def dis_products(df, bucket_name, save_path):
    try:
        discounted_products_count = df.filter(col("PercentageDiscount") > 0) \
                                 .groupBy("Category") \
                                 .count()
        discounted_products_count.write.parquet(f"s3a://{bucket_name}/{save_path}")
        logging.info(f'Data successfully written to s3a://{bucket_name}/{save_path}')
    except Exception as e:
        logging.error(f'Failed to write data to S3: {e}')

# Most expensive products on Carrefour that day
def top_expensive(df, bucket_name, save_path):
    try:
        top_expensive_products = df.orderBy(col("ApplicablePrice").desc()).limit(10)
        top_expensive_products.write.parquet(f"s3a://{bucket_name}/{save_path}")
        logging.info(f'Data successfully written to s3a://{bucket_name}/{save_path}')
    except Exception as e:
        logging.error(f'Failed to write data to S3: {e}')

# 10 cheapest products on Carrefour that day
def top_cheap(df, bucket_name, save_path):
    try:
        top_cheap_products = df.orderBy(col("ApplicablePrice").asc()).limit(10)
        top_cheap_products.write.parquet(f"s3a://{bucket_name}/{save_path}")
        logging.info(f'Data successfully written to s3a://{bucket_name}/{save_path}')
    except Exception as e:
        logging.error(f'Failed to write data to S3: {e}')
                
if __name__ == '__main__':
    # .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.0,com.amazonaws:aws-java-sdk-bundle:1.12.734") \
    jars = "jar_files/aws-java-sdk-bundle-1.11.1026.jar, jar_files/hadoop-aws-3.3.2.jar"       
    spark = SparkSession.builder \
        .master('local') \
        .appName('c4') \
        .config("spark.jars", jars) \
        .getOrCreate()
    
    access_key = os.getenv('AWS_ACCESS_KEY_ID')
    secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", access_key)
    hadoop_conf.set("fs.s3a.secret.key", secret_key)
    hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
    hadoop_conf.set("com.amazonaws.services.s3.enableV4", "true")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "true")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    
    df = spark.read.parquet('data/pq/all_foods')
    bucket_name = 'c4-analysed'

    avg_dis_to_s3(df, bucket_name, 'avg-discount.parquet')
    highest_dis(df, bucket_name, 'highest_discounted_product.parquet')
    lowest_dis(df, bucket_name, 'lowest_discounted_product.parquet')
    price_dist(df, bucket_name, 'price_distribution.parquet')
    dis_products(df, bucket_name, 'discounted_products_count.parquet')
    top_expensive(df, bucket_name, 'top_expensive_products.parquet')
    top_cheap(df, bucket_name, 'top_cheap_products.parquet')
    spark.stop()


