import pandas as pd
from urllib.parse import urlparse
import os
import s3fs

def fetch(url: str) -> str:
    """Downloaading the dataset"""
    parsed_url = urlparse(url)
    # print(parsed_url)     
    file_name = os.path.basename(parsed_url.path)
    # print(f'file_name: {file_name}')
    if url.endswith('.csv.gz'):
        csv_name = f'./data/{file_name}'
    else:
        csv_name = f'./data/{file_name}'
    os.system(f'wget {url} -O {csv_name}')
    return csv_name

if __name__ == '__main__':
    fetch('https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-05.csv.gz')
