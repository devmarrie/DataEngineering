from pathlib import Path
import os
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task()
def extract_data_from_gcs() -> Path:
    """Fetch data  from the bucket"""
    gcs_path = Path('data/yellow/yellow.parquet')
    gcs_block = GcsBucket.load("my-nyc-taxi-bucket")
    gcs_block.get_directory(from_path=gcs_path, local_path=f'../data/')
    return Path(f'../data/{gcs_path}')

@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    """Removes the data whose passanger count is 0"""
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    return df

@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write the clean data to big query"""
    gcp_credentials_block = GcpCredentials.load("ny-taxi-cred")
    df.to_gbq(
        destination_table="trips_data_all.nyc_rides_data",
        project_id="arcane-grin-394118",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500,
        if_exists="append"
    )

@flow()
def cloud_bucket_to_bq():
    """The flow updating data from data lake to data warehouse"""
    path = extract_data_from_gcs()
    df = transform(path)
    write_bq(df)

if __name__ == '__main__':
    cloud_bucket_to_bq()
