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

@task(log_prints=True)
def extract_transform_load_to_path(dataset_url: str, path: Path) -> Path:
    """Once data has been fetched from url, it reads the data"""
    df_chunks = pd.read_csv(dataset_url, compression='gzip', iterator=True, chunksize=50000)
 
    pth = path.stem
    file_name = pth.split(".")[0]
    for i, chunk in enumerate(df_chunks):
        chunk_path = f'data/green/{file_name}_chunk_{i}.parquet.gz'
        chunk["lpep_pickup_datetime"]	= pd.to_datetime(chunk["lpep_pickup_datetime"])
        chunk["lpep_dropoff_datetime"] = pd.to_datetime(chunk["lpep_dropoff_datetime"])
        chunk.to_parquet(chunk_path, index=False, compression='gzip')
        print(f'Inserted chunk {i+1}')
    print('After insertion:')
    return chunk_path

@task()
def load(path: Path, color: str, year: int, month: int):
    """Load the google cloud storage bucket """
    folder = "data/green"
    start = f"{color}_tripsdata_{year}_{month:02}"
    cloud_bucket = GcsBucket.load("my-nyc-taxi-bucket")

    if os.path.exists(folder) and os.path.isdir(folder):
        for file in os.listdir(folder):
            file_path = os.path.join(folder, file)
            if file.startswith(start):
                print(f"Uploading: {file} to {file_path}")
                cloud_bucket.upload_from_path(from_path=file_path, to_path=file_path)
    return

@flow()
def web_to_gcp(month: int, color: str, year: int) -> None:
    """Loads data to the data lake"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    path =  Path(f"data/{color}_tripsdata_{year}_{month:02}.parquet.gz")
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    data = fetch(dataset_url)
    extract_transform_load_to_path(data, path)
    load(path,color, year, month)

@flow()
def diff_months(months: list[int] = [1, 2], year: int = 2019, color: str = "yellow"):
    """More than one data sources"""
    for month in months:
        web_to_gcp(month, color, year)

if __name__ == '__main__':
    color = "green"
    year = 2020
    months = [11, 12]
    diff_months(months, year, color)
