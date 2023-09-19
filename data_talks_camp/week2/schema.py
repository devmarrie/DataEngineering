import pyarrow.parquet as pq

# Read the Parquet file
parquet_file = pq.ParquetFile('data/yellow_tripsdata_2019_04_chunk_29.parquet.gz')

# Get the schema of the Parquet file
schema = parquet_file.schema

# Print the schema
print(schema)