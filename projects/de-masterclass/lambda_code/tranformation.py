import json
import pandas as pd
import boto3
import ast
from io import StringIO
import s3_file_operations as s3_ops

# Transform origin_id and location_id into just ints and not json then drop the old columns

def character_trans(characters_df):
    # Function to extract the ID from a URL
    extract_id = lambda x: x.split('/')[-1] if x else None

    # Using list comprehension to extract origin_id and location_id
    characters_df['origin_id'] = [
        extract_id(ast.literal_eval(record)['url']) if isinstance(record, str) else None
        for record in characters_df['origin']
    ]

    characters_df['location_id'] = [
        extract_id(ast.literal_eval(record)['url']) if isinstance(record, str) else None
        for record in characters_df['location']
    ]
    
    # Drop and rename columns
    print("Dropping and renaming columns...")
    characters_df = characters_df.drop(columns=['origin', 'location', 'episode'])

    characters_df.info()
    return characters_df
    
# Appearance Table 
def appearance_trans(episodes_df):
    appearance_df = episodes_df.copy()

    # Function to extract the ID from a URL
    character_func = lambda x: [url.split('/')[-1] for url in ast.literal_eval(x)] if isinstance(x, str) else None

    # Using list comprehension to extract character_ids
    appearance_df['character_ids'] = [
        character_func(record) if record else None
        for record in appearance_df['characters']
    ]

    # Explode the 'character_ids' column to create a row for each character ID
    expanded_df = appearance_df.explode('character_ids')

    # Reset the index to create a new 'id' column
    expanded_df = expanded_df.reset_index(drop=True).reset_index().rename(columns={'index': 'id_new'})

    # Rename columns to match the desired output
    expanded_df = expanded_df.rename(columns={'id_new': 'id', 'id': 'episode_id', 'character_ids': 'character_id'})

    # Select only the relevant columns
    expanded_df = expanded_df[['id', 'episode_id', 'character_id']]

    print(expanded_df.head())
    return expanded_df

def lambda_handler(event, context):
    bucket = "de-masterclass-ricknmorty"  # S3 bucket name

    # Read data from S3
    print("Reading Character data from S3...")
    characters_df = s3_ops.read_csv_from_s3(bucket, 'Rick&Morty/Untransformed/Character.csv')
    
    print("Reading Episode data from S3...")
    episodes_df = s3_ops.read_csv_from_s3(bucket, 'Rick&Morty/Untransformed/Episode.csv')
    
    print("Reading Location data from S3...")
    location_df = s3_ops.read_csv_from_s3(bucket, 'Rick&Morty/Untransformed/Location.csv')
    
    
    # Check if data is loaded successfully
    if characters_df is None or episodes_df is None or location_df is None:
        print("Error in loading data from S3")
    else:
        print(f"Characters DataFrame shape: {characters_df.shape}")
        print(f"Episodes DataFrame shape: {episodes_df.shape}")
        print(f"Locations DataFrame shape: {location_df.shape}")
    
    xcter_df = character_trans(characters_df)
    
    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    expanded_df = appearance_trans(episodes_df)
    
    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    # we can now safely drop the characters column from our Episodes dataframe
    episodes_df = episodes_df.drop("characters", axis=1)
    episodes_df.info()
    
    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    # Dropping Residents from Location Table
    location_df = location_df.drop('residents', axis=1)
    location_df.info()
    
    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~SAVING~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    # Saving our final Dataframes to s3

    # Character DataFrame
    s3_ops.write_data_to_s3(xcter_df, bucket, 'Rick&Morty/Transformed/Character.csv')
    # Episodes DataFrame
    s3_ops.write_data_to_s3(episodes_df, bucket, 'Rick&Morty/Transformed/Episode.csv')
    # Appearance DataFrame
    s3_ops.write_data_to_s3(expanded_df, bucket, 'Rick&Morty/Transformed/Appearance.csv')
    # Location DataFrame
    s3_ops.write_data_to_s3(location_df, bucket, 'Rick&Morty/Transformed/Location.csv')

    
    print("Data loaded successfully from S3")
    
    return {
    'statusCode': 200,
    'body': json.dumps('Data transformation and upload successful')
    }