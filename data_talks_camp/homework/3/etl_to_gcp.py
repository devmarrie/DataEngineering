from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket
from datetime import timedelta


@task(retries=3, log_prints=True, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str, path: Path) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    df_chunks = pd.read_csv(dataset_url,compression='gzip', iterator=True, chunksize=500000)
    df = next(df_chunks)
    df.pickup_datetime = pd.to_datetime(df.pickup_datetime)
    df.dropOff_datetime = pd.to_datetime(df.dropOff_datetime)
    df.to_parquet(path, index=False)

    for i, chunk in enumerate(df_chunks, start=1):
        existing = pd.read_parquet(path)
        chunk.pickup_datetime = pd.to_datetime(chunk.pickup_datetime)
        chunk.dropOff_datetime = pd.to_datetime(chunk.dropOff_datetime)
        combined = pd.concat([existing, chunk], ignore_index=True)
        combined.to_parquet(path, index=False)
        print(f'Inserted chunk {i}')
    print('Chunks inserted successfully')
    return path

@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("my-nyc-taxi-bucket")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def new_web_to_gcs() -> None:
    """The main ETL function"""
    month = 1
    path = Path(f"data/2019/fhv_{month:02}.parquet")
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-{month:02}.csv.gz"
    

    path = fetch(dataset_url, path)
    # path = loop_clean(df_chunks, path)
    write_gcs(path)


if __name__ == "__main__":
    new_web_to_gcs()