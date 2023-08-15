#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from time import time
import os
from sqlalchemy import create_engine
from prefect import flow, task

@task(log_prints=True, retries=3)
def home(user, password, host, port, db, table_name, url):

    # user = kwargs.user
    # password = kwargs.password
    # host = kwargs.host
    # port = kwargs.port
    # db = kwargs.db
    # table_name = kwargs.table
    # url = kwargs.url

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


    # while True:
    #     try:
    #        t_start = time()
    #        df = next(df_batch)
    #        df.tpep_pickup_datetime	= pd.to_datetime(df.tpep_pickup_datetime)
    #        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    #        df.to_sql(name=table_name, con=engine, if_exists='append')
    #        t_end = time()
    #        print(f'Inserted another chunk which took {t_end - t_start} sec')
    #     except StopIteration:
    #         print(f'Finished ingesting the {table_name} data')
    #         break

@flow(name="Ingest Flow")
def main():
    user = 'postgres'
    password = 'password'
    host = 'localhost'
    port = 5432
    db = 'ny_taxi'
    table_name = 'yellow_taxi_data'
    url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2019-01.csv.gz'
    
    home(user, password, host, port, db, table_name, url)

if __name__ == '__main__':
    main()
