from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket
from datetime import timedelta


# @task(retries=3, log_prints=True, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
# def fetch_data(dataset_url: str, path: Path) -> Path:
#     """Read taxi data from web into pandas DataFrame"""
#     df_chunks = pd.read_csv(dataset_url,compression='gzip', iterator=True, chunksize=500000)
#     df = next(df_chunks)
#     df.pickup_datetime = pd.to_datetime(df.pickup_datetime)
#     df.dropOff_datetime = pd.to_datetime(df.dropOff_datetime)
#     print('First chunk only:')
#     print(df.shape[0])

#     for i, chunk in enumerate(df_chunks, start=1):
#         chunk.pickup_datetime = pd.to_datetime(chunk.pickup_datetime)
#         chunk.dropOff_datetime = pd.to_datetime(chunk.dropOff_datetime)
#         df = pd.concat([df, chunk], ignore_index=True)
#         print(f'Inserted chunk {i+1}')
#     print('After insertion:')
#     print(df.shape[0])
#     df.to_parquet(path, index=False, compression='gzip')
#     print("inserted data to local path")
#     return path

@task()
def read_in_batches(path: Path) -> None:
    pq_file = pq.ParquetFile(path)
    gcs_block = GcsBucket.load("my-nyc-taxi-bucket")
    for i,batch in enumerate(pq_file.iter_batches()):
        df = batch.to_pandas()
        gcs_block.upload_from_dataframe(df=df, to_path=path, serialization_format='parquet_gzip')
        print(f'Inserted batch {i+1}: {df.shape}')
    return  

@flow()
def web_to_gcs_flow(month: int) -> None:
    """The main ETL function"""
    path = Path(f"data/2019/fhv_{month:02}.parquet.gz")
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-{month:02}.csv.gz"
    # p = fetch_data(dataset_url, path)
    read_in_batches(path)
    

# @flow()
# def multiple_mnths():
#     months = [1,2,3,4,5,6,7,8,9,10,11,12]
#     for month in months:
#         web_to_gcs_flow(month)


if __name__ == "__main__":
    web_to_gcs_flow(5)