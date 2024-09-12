import json
import os
import pandas as pd
import numpy as np
import psycopg2
import s3_file_operations as s3_ops

rds_host = "********eu-west-1.rds.amazonaws.com" #rds endpoint
rds_username = "postgres" # rds
rds_user_pwd ="******" #from rds
rds_db_name = "*****" #create a db in pgadmin and put it here
bucket_name = "******"   # Replace with your s3 Bucket name

def insert_data(cursor, conn, df, table_name):
    column_names = list(df.columns)
    
    # Convert all numpy types to native Python types
    df = df.astype(object).where(pd.notnull(df), None)  # Replace NaN with None
    
    for i, row in df.iterrows():
        placeholders = ','.join(['%s'] * len(column_names))
        sql_insert = f"INSERT INTO {table_name} ({','.join(column_names)}) VALUES ({placeholders});"
        # Convert numpy.int64 to int and numpy.float64 to float before inserting
        data = tuple(row[column].item() if isinstance(row[column], (np.int64, np.float64)) else row[column] for column in column_names)
        cursor.execute(sql_insert, data)
        conn.commit()

def lambda_handler(event, context):
    # Read transformed data from S3
    print("Reading transformed data from S3...")
    characters_df = s3_ops.read_csv_from_s3(bucket_name, 'Rick&Morty/Transformed/Character.csv')
    episodes_df = s3_ops.read_csv_from_s3(bucket_name, 'Rick&Morty/Transformed/Episode.csv')
    appearance_df = s3_ops.read_csv_from_s3(bucket_name, 'Rick&Morty/Transformed/Appearance.csv')
    location_df = s3_ops.read_csv_from_s3(bucket_name, 'Rick&Morty/Transformed/Location.csv')

    # Check if data is loaded successfully
    if characters_df is None or episodes_df is None or appearance_df is None or location_df is None:
        print("Error in loading data from S3")
        return {
            'statusCode': 500,
            'body': json.dumps('Error in loading data from S3')
        }

    print("Data loaded successfully from S3")

    # SQL create table scripts
    create_character_table = """
        CREATE TABLE IF NOT EXISTS Character_Table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            status VARCHAR(255),
            species VARCHAR(255),
            type VARCHAR(255),
            gender VARCHAR(255),
            origin_id VARCHAR(255),
            location_id VARCHAR(255),
            image VARCHAR(255),
            url VARCHAR(255),
            created TIMESTAMPTZ
        );
    """

    create_episode_table = """
        CREATE TABLE IF NOT EXISTS Episode_Table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            air_date VARCHAR(255),
            episode VARCHAR(255),
            url VARCHAR(255),
            created TIMESTAMPTZ
        );
    """

    create_appearance_table = """
        CREATE TABLE IF NOT EXISTS Appearance_Table (
            id SERIAL PRIMARY KEY,
            episode_id INT,
            character_id INT
        );
    """

    create_location_table = """
        CREATE TABLE IF NOT EXISTS Location_Table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            type VARCHAR(255),
            dimension VARCHAR(255),
            url VARCHAR(255),
            created TIMESTAMPTZ
        );
    """

    try:
        print(f"Connecting to RDS instance at {rds_host} with username {rds_username}")
        conn = psycopg2.connect(
            host=rds_host,
            user=rds_username,
            password=rds_user_pwd,
            dbname=rds_db_name,
            port=5432
        )
        cursor = conn.cursor()

        # Create tables
        cursor.execute(create_character_table)
        cursor.execute(create_episode_table)
        cursor.execute(create_appearance_table)
        cursor.execute(create_location_table)

        # Insert data into Character_Table
        insert_data(cursor, conn, characters_df, "Character_Table")

        # Insert data into Episode_Table
        insert_data(cursor, conn, episodes_df, "Episode_Table")

        # Insert data into Appearance_Table
        insert_data(cursor, conn, appearance_df, "Appearance_Table")

        # Insert data into Location_Table
        insert_data(cursor, conn, location_df, "Location_Table")

        print("Data insertion completed successfully")

    except Exception as e:
        print("Exception: ", e)
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }

    return {
        'statusCode': 200,
        'body': json.dumps('Data transformation and upload successful')
    }


