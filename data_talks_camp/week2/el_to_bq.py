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

@task(retries=3)
def extract(dataset_url: str) -> pd.DataFrame:
    """Once data has been fetched from url, it reads the data"""
    df_chunk = pd.read_csv(dataset_url, iterator=True, chunksize=50000)
    df = next(df_chunk)
    return df

@task(log_prints=True)
def transform(df: pd.DataFrame) -> pd.DataFrame:
    df["tpep_pickup_datetime"]	= pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    print(df.head(5))
    print(f'columns: {df.dtypes}')
    print(f'rows: {len(df)}')
    return df

@task(log_prints=True)
def to_parque(df: pd.DataFrame) -> Path:
    """From csv to parque"""
    path = Path('data/yellow/yellow.parquet')
    df.to_parquet(path, compression="gzip")
    print(df.head(5))
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
    df = extract(data)
    cleaned =transform(df)
    trans = to_parque(cleaned)
    load(trans)

@flow()
def diff_months(months: list[int] = [1, 2], year: int = 2021, color: str = "yellow"):
    """More than one data sources"""
    for month in months:
        web_to_gcp(month, color, year)

if __name__ == '__main__':
    color = "yellow"
    year = 2021
    months = [1,2,3]
    diff_months(months, year, color)
