#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from time import time
import argparse
import os
from sqlalchemy import create_engine

def home(params):

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table
    url = 'params.url'

    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f'wget {url} -O {csv_name}')

    # cur_dir = os.getcwd()
    # csv_name = os.path.join(cur_dir, "yellow_tripdata_2021-01.csv")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_batch = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_batch)

    df.tpep_pickup_datetime	= pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')


    while True:
        try:
           t_start = time()
           df = next(df_batch)
           df.tpep_pickup_datetime	= pd.to_datetime(df.tpep_pickup_datetime)
           df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
           df.to_sql(name=table_name, con=engine, if_exists='append')
           t_end = time()
           print(f'Inserted another chunk which took {t_end - t_start} sec')
        except StopIteration:
            print(f'Finished ingesting the {table_name} data')
            break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Ingest NY CSV data.")
    parser.add_argument("--user", help="Postgres user name")
    parser.add_argument("--password", help="Postgres password")
    parser.add_argument("--host", help="Postgres host")
    parser.add_argument("--port", help="Postgres port")
    parser.add_argument("--db", help="Postgres db")
    parser.add_argument("--table_name", help="Postgres table")
    parser.add_argument('--url', required=True, help='url of the csv file')
    args = parser.parse_args()
    home(args)

