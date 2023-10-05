from pathlib import Path
import os
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task()
def extract_data_from_gcs(color: str, year: int, month: int, chunk: int) -> Path:
    """Fetch data  from the bucket"""           
    gcs_path = f'data/{color}/{color}_tripsdata_{year}_{month:02}_chunk_{chunk}.parquet.gz'
    gcs_block = GcsBucket.load("my-nyc-taxi-bucket")
    try:
       gcs_block.get_directory(from_path=gcs_path, local_path=f'data/{color}')
    except Exception as e:
        print(f'File {gcs_path} not found: {str(e)}')
        return None
    return Path(f'{gcs_path}')

@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    """Removes the data whose passanger count is 0"""
    if path is None or not os.path.exists(path):
        print(f"File not found for {path}")
        return None
    
    df = pd.read_parquet(path)
    print(f'Onload: {df.columns}')
    df.rename(columns={"lpep_pickup_datetime  " : "lpep_pickup_datetime"}, inplace=True)
    print(f'Before: {df.dtypes}')
    df["lpep_pickup_datetime"]= pd.to_datetime(df["lpep_pickup_datetime"])
    df.drop('lpep_pickup_datetime\t', axis=1, inplace=True)
    print(f'After: {df.dtypes}')
    print(f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    return df
@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write the clean data to big query"""
    gcp_credentials_block = GcpCredentials.load("ny-taxi-cred")
    if df is None and df.empty:
        print("No data to push to BigQuery")

    df.to_gbq(
        destination_table="trips_data_all.nyc_green_tripdata",
        project_id="arcane-grin-394118",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=25000,
        if_exists="append"
    )

@flow()
def etl_gcs_to_bq(month: int, chunk: int):
    """The flow updating data from data lake to data warehouse"""
    color = "green"
    year = 2020
    path = extract_data_from_gcs(color, year, month, chunk)
    if path is not None:
        df = transform(path)
        if df is not None:
            write_bq(df)

flow()
def multiple_chunk_months():
    for month in range(7,12):
        for chunk in range(0,51):
            etl_gcs_to_bq(month, chunk)

if __name__ == '__main__':
   multiple_chunk_months()
