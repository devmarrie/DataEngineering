from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket
from datetime import timedelta


@task(retries=3, log_prints=True, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str, path: Path) -> Path:
    """Read taxi data from web into pandas DataFrame"""
    df_chunks = pd.read_csv(dataset_url,compression='gzip', iterator=True, chunksize=500000)
    df = next(df_chunks)
    df.pickup_datetime = pd.to_datetime(df.pickup_datetime)
    df.dropOff_datetime = pd.to_datetime(df.dropOff_datetime)
    print('First chunk only:')
    print(df.shape[0])

    for i, chunk in enumerate(df_chunks, start=1):
        chunk.pickup_datetime = pd.to_datetime(chunk.pickup_datetime)
        chunk.dropOff_datetime = pd.to_datetime(chunk.dropOff_datetime)
        df = pd.concat([df, chunk], ignore_index=True)
        print(f'Inserted chunk {i+1}')
    print('After insertion:')
    print(df.shape[0])
    df.to_parquet(path, index=False, compression='gzip')
    print("inserted data to local path")
    return path

@task()
def read_parquet_file_in_batches(path: Path) -> None:
    pq_file = pq.ParquetFile(path)
    lst = []
    for i,batch in enumerate(pq_file.iter_batches()):
        df = batch.to_pandas()
        print(f'Inserted batch {i+1}: {df.shape}')
        lst.append(df)
    return lst
        

@task()
def write_to_gcs(path: Path, lst: list) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("my-nyc-taxi-bucket")
    for df in lst:
        gcs_block.upload_from_dataframe(df=df, to_path=path, serialization_format='parquet_gzip')
        print(f'Inserted: {df.shape}')
    return


@flow()
def new_web_to_gcs(month: int) -> None:
    """The main ETL function"""
    path = Path(f"data/2019/fhv_{month:02}.parquet")
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-{month:02}.csv.gz"
    path = fetch(dataset_url, path)
    lst = read_parquet_file_in_batches(path)
    write_to_gcs(path, lst)

# @flow()
# def multiple_mnths():
#     months = [1,2,3,4,5,6,7,8,9,10,11,12]
#     for month in months:
#         new_web_to_gcs(month)


if __name__ == "__main__":
    new_web_to_gcs(1)