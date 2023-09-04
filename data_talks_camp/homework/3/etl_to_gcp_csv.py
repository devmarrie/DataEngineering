from pathlib import Path
import pandas as pd
import os
import pyarrow as pa
import pyarrow.parquet as pq
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket
from datetime import timedelta


@task(retries=3, log_prints=True, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch_data_csv(dataset_url: str, path: Path) -> Path:
    """Read taxi data from web into pandas DataFrame"""
    df_chunks = pd.read_csv(dataset_url,compression='gzip', iterator=True, chunksize=25000)
    pth = path.stem
    file_name = pth.split(".")[0]
    
    for i, chunk in enumerate(df_chunks):
        chunk_path = Path(f'data/2019_csv/{file_name}_chunk_{i}.csv.gz')
        chunk.pickup_datetime = pd.to_datetime(chunk.pickup_datetime)
        chunk.dropOff_datetime = pd.to_datetime(chunk.dropOff_datetime)
        chunk.to_csv(chunk_path, index=False, header=False, mode='a', compression='gzip')
        print(f'Inserted chunk {i+1}')
    return chunk_path
    

# @task()
# def read_in_batches_csv(month: int) -> None:
#     """Upload local csv file to GCS"""
#     gcs_block = GcsBucket.load("my-nyc-taxi-bucket")
#     folder = "data/2019_csv"
#     start = f"fhv_{month:02}"
#     if os.path.exists(folder) and os.path.isdir(folder):
#         for file in os.listdir(folder):
#             file_path = os.path.join(folder, file)
#             if file.startswith(start):
#                 print(f"Uploading: {file} to {file_path}")
#                 gcs_block.upload_from_path(from_path=file_path, to_path=file_path)
#     return



@flow()
def web_to_gcs_flow_csv(month: int) -> None:
    """The main ETL function"""
    path = Path(f"data/2019_csv/fhv_{month:02}.csv.gz")
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-{month:02}.csv.gz"
    fetch_data_csv(dataset_url, path)
    # read_in_batches_csv(month)
    

# @flow()
# def multiple_mnths_csv():
#     months = [1,2,3,4,5,6,7,8,9,10,11,12]
#     for month in months:
#         web_to_gcs_flow_csv(month)


if __name__ == "__main__":
    web_to_gcs_flow_csv(1)
    