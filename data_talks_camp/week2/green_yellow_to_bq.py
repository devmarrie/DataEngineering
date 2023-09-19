from pathlib import Path
import os
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task()
def extract_data_from_gcs(color: str, year: int, month: int, chunk: int) -> Path:
    """Fetch data  from the bucket"""           
    gcs_path = f'data/{color}_tripsdata_{year}_{month:02}_chunk_{chunk}.parquet.gz'
    gcs_block = GcsBucket.load("my-nyc-taxi-bucket")
    gcs_block.get_directory(from_path=gcs_path, local_path=f'data/')
    return Path(f'{gcs_path}')

@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    """Removes the data whose passanger count is 0"""
    if os.path.exists(path):
        df = pd.read_parquet(path)
        print(f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
        df = df[df['passenger_count'] != 0]
        print(f"post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
        return df
    else:
        print(f'File not found for {path}')

@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write the clean data to big query"""
    gcp_credentials_block = GcpCredentials.load("ny-taxi-cred")
    df.to_gbq(
        destination_table="trips_data_all.nyc_yellow_tripdata",
        project_id="arcane-grin-394118",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=25000,
        if_exists="append"
    )

@flow()
def etl_gcs_to_bq(month: int, chunk: int):
    """The flow updating data from data lake to data warehouse"""
    color = "yellow"
    year = 2020
    path = extract_data_from_gcs(color, year, month, chunk)
    df =  transform(path)
    write_bq(df)

flow()
def multiple_chunk_months():
    for month in range(1,13):
        for chunk in range(0,200):
            etl_gcs_to_bq(month, chunk)

if __name__ == '__main__':
   multiple_chunk_months()
