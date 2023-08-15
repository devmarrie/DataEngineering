#!/usr/bin/env python

import pandas as pd
from sqlalchemy import create_engine
import argparse

def green_data(params):
   user = params.user
   password = params.password
   host = params.host
   port = params.port
   db = params.db
   table = params.table
   df_batch = pd.read_csv('green_tripdata_2019-01.csv', iterator=True, chunksize=100000)
   df = next(df_batch)
   len(df)
   df.head()
   df.lpep_pickup_datetime	= pd.to_datetime(df.lpep_pickup_datetime)
   df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
   df.iloc[6]
   engine = create_engine('postgresql://postgres:password@localhost:5432/ny_green_data')
   engine.connect()

   print(pd.io.sql.get_schema(df, name='green_taxi_data', con=engine))

   df.head(n=0).to_sql(name='green_taxi_data', con=engine, if_exists='replace')

   count = 1
   while True:
        try:
           df = next(df_batch)
           df.lpep_pickup_datetime	= pd.to_datetime(df.lpep_pickup_datetime)
           df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
           df.to_sql(name='green_taxi_data', con=engine, if_exists='append')
           print(f'Inserted batch {count}')
           count += 1
        except StopIteration:
            break

# Zones dataset
   df_zones = pd.read_csv('taxi+_zone_lookup.csv', iterator=True, chunksize=50)
   df2 = next(df_zones)
   df2.head()
   print(pd.io.sql.get_schema(df2, name='taxi_zones', con=engine))
   df2.head(n=0).to_sql(name='taxi_zones', con=engine, if_exists='replace')
   count = 1
   while True:
       try:
          df2 = next(df_zones)
          df2.to_sql(name='taxi_zones', con=engine, if_exists='append')
          print(f'Inserted zone {count}')
          count += 1
       except StopIteration:
          break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Ingest NY CSV data.")
    parser.add_argument("--user", help="Postgres user name")
    parser.add_argument("--password", help="Postgres password")
    parser.add_argument("--host", help="Postgres host")
    parser.add_argument("--port", help="Postgres port")
    parser.add_argument("--db", help="Postgres db")
    parser.add_argument("--table", help="Postgres table")
    args = parser.parse_args()
    green_data(args)

