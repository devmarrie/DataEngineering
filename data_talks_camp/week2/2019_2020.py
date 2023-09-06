from pathlib import Path
import os
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta

@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(url):
    """Fetch the data from github"""
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'
    os.system(f'wget {url} -O {csv_name}')
    return csv_name

@task(retries=3, log_prints=True)
def extract_transform_load_to_path(dataset_url: str) -> Path:
    """Once data has been fetched from url, it reads the data"""
    df_chunk = pd.read_csv(dataset_url, iterator=True, chunksize=50000)
    path = Path('data/yellow.parquet')
    df = next(df_chunk)
    df["tpep_pickup_datetime"]	= pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    print("First chunk")
    df.to_parquet(path, compression="gzip")
    
    while True:
        try:
           df = next(df_chunk)
           df["tpep_pickup_datetime"]	= pd.to_datetime(df["tpep_pickup_datetime"])
           df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
           print("Inserted another batch")
           df.to_parquet(path, compression="gzip")
        except StopIteration:
            print(f"The whole dataset has been loaded succesfully to {path}")
            break
    return path

@task()
def load(path: Path):
    """Load the google cloud storage bucket """
    cloud_bucket = GcsBucket.load("my-nyc-taxi-bucket")
    cloud_bucket.upload_from_path(from_path=path, to_path=path)
    return

@flow()
def web_to_gcp(month: int, color: str, year: int) -> None:
    """Loads data to the data lake"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    data = fetch(dataset_url)
    extract_transform_load_to_path(data)
    # load(trans)

@flow()
def diff_months(months: list[int] = [1, 2], year: int = 2019, color: str = "yellow"):
    """More than one data sources"""
    for month in months:
        web_to_gcp(month, color, year)

if __name__ == '__main__':
    color = "yellow"
    year = 2021
    months = [1,2,3]
    diff_months(months, year, color)
