import pandas as pd
from urllib.parse import urlparse
import os
import s3fs
import logging
import pyarrow.csv as pv
import pyarrow.parquet as pq
from pathlib import Path

# def fetch(url: str) -> str:
#     """Downloaading the dataset"""
#     parsed_url = urlparse(url)
#     # print(parsed_url)     
#     file_name = os.path.basename(parsed_url.path)
#     # print(f'file_name: {file_name}')
#     if url.endswith('.csv.gz'):
#         csv_name = f'./data/{file_name}'
#     else:
#         csv_name = f'./data/{file_name}'
#     # os.system(f'wget {url} -O {csv_name}')
#     # return csv_name
#     df = pd.read_csv(url)
#     df.to_csv(f's3://nytaxi-data-raw-us-east-1-dev/{csv_name}')



def read_from_source(filename: str):
    """Convert the csv file to pq"""
    # if not filename.endswith('csv.gz'):
    #     logging.error("Can only accept csv format at the momment")
    table = pv.read_csv(filename)
    pq.write_table(table, filename.replace('.csv', '.parquet'))

if __name__ == '__main__':
    data_file = 'yellow_tripdata_2021-05.csv.gz'
    url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/{data_file}'
    filename = f'data/{data_file}'
    read_from_source(filename)
