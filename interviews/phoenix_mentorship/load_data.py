# load the data to a remote postgres db 

import os
import psycopg2
import pandas as pd
from psycopg2 import sql

def load_credentials(file_path):
    cred = {}
    with open(file_path, 'r') as f:
        for line in f:
            if '->' in line:
                key, value = line.strip().split('->')
                cred[key.strip()] = value.strip()
    return cred


def create_table(conn):
    table_query = """
    CREATE TABLE IF NOT EXISTS cleaned_agric_data (
        id SERIAL PRIMARY KEY,
        commodity TEXT,
        market TEXT,
        wholesale FLOAT,
        retail FLOAT,
        supply_volume FLOAT,
        county TEXT,
        date DATE
    );
    """
    try:
        with conn.cursor() as cur:
            cur.execute(table_query)
            conn.commit()
            print("Table created successfully")
    except Exception as e:
        print(f"Failed to create table: {e}")
        conn.rollback()
    finally:
        if cur:
            cur.close

def insert_data(df, conn):
    try:
        with conn.cursor() as cur:
            for index, row in df.iterrows():
                insert_query = sql.SQL("""
                    INSERT INTO cleaned_agric_data (commodity, market, wholesale, retail, supply_volume, county, date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                """)
                cur.execute(insert_query, tuple(row))
            conn.commit()
            print(f"{len(df)} rows inserted successfully")
    except Exception as e:
        print(f"Failed to insert data: {e}")
        conn.rollback()
    finally:
        if cur:
            cur.close



if __name__ == '__main__':
    credentials = load_credentials('credentials.txt')
    df = pd.read_csv('data/clean/cleaned_agric_data.csv')
    try:
        conn = psycopg2.connect(
            host = credentials['host'],
            database = credentials['database_name'],
            user = credentials['user'],
            password = credentials['password'],
            port = credentials['port']
        )
        print("Connection Successful")
        create_table(conn)
        insert_data(df, conn)
    except Exception as e:
        print(f'Failed to connect:{e}')
    finally:
        if conn:
            conn.close()




