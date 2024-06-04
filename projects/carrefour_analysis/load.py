import logging
import boto3
import os
from dotenv import load_dotenv
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession

# load environment variables
load_dotenv()

def load_to_s3(spark, df, bucket_name, save_path, file_format="parquet", options={}):
    # confgure s3 env
    try:
        avg_discount = df.groupBy('Category').agg({"PercentageDiscount": "avg"}).withColumnRenamed("avg(PercentageDiscount)", "AverageDiscount")
        avg_discount.write \
                    .format(file_format) \
                    .options(**options) \
                    .save(f"s3a://{bucket_name}/{save_path}")
        logging.info(f'Data successfully written to s3a://{bucket_name}/{save_path}')
    except Exception as e:
        logging.error(f'Failed to write data to S3: {e}')
        

def create_bucket(bucket_name, region):
    try:
        s3_client = boto3.client('s3')
        location = {'LocationConstraint': region}
        s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)
    except ClientError as e:
        logging.error(e)

    return f'{bucket_name} created successfully'


if __name__ == '__main__':
    jars = ["hadoop-aws-3.4.0.jar", "s3-transfer-manager-2.25.64-javadoc.jar"]         
    spark = SparkSession.builder \
        .master('local') \
        .appName('c4') \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.0,com.amazonaws:aws-java-sdk-bundle:1.12.734") \
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
    bucket_name = 'c4-load'

    # create_bucket(bucket_name, 'af-south-1')
    load_to_s3(spark, df, bucket_name, 'avg_discount.parquet')

    spark.stop()


