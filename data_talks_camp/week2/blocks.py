
import pandas as pd
from time import time
import os
from sqlalchemy import create_engine
from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector

@task(log_prints=True, retries=3)
def home(table_name, url):

    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f'wget {url} -O {csv_name}')

    df_batch = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_batch)

    df.tpep_pickup_datetime	= pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    # engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    
    conn_block = SqlAlchemyConnector.load("nyc-data")
    with conn_block.get_connection(begin=False) as engine:
       df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

       df.to_sql(name=table_name, con=engine, if_exists='append')

@flow(name="Ingest Flow")
def main():
    # user = 'postgres'
    # password = 'password'
    # host = 'localhost'
    # port = 5432
    # db = 'ny_taxi'
    table_name = 'yellow_taxi_data'
    url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2019-01.csv.gz'
    
    home(table_name, url)

if __name__ == '__main__':
    main()
