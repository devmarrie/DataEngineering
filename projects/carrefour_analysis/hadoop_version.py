from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .master('local[*]') \
    .appName('Check Hadoop Version') \
    .getOrCreate()

# Get Hadoop version
hadoop_version = spark.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion()
print(f'Hadoop version: {hadoop_version}')

# Stop Spark session
spark.stop()