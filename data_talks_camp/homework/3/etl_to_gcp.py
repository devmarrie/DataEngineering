from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket
from datetime import timedelta


@task(retries=3, log_prints=True, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    df_chunks = pd.read_csv(dataset_url,compression='gzip', iterator=True, chunksize=100000)
    return df_chunks


@task(log_prints=True)
def loop_clean(df_chunks: pd.DataFrame, path: Path) -> Path:
    """Fix dtype issues and loop"""
    for i, chunk in enumerate(df_chunks):
        print(f'Processing batch {i+1}')
        chunk.pickup_datetime = pd.to_datetime(chunk.pickup_datetime)
        chunk.dropOff_datetime = pd.to_datetime(chunk.dropOff_datetime)
        if i == 0:
            # to an arrow table
            table = pa.Table.from_pandas(chunk)
            pq.write_table(table, path, compression='gzip')
        else:
           table = pa.Table.from_pandas(chunk)
           with pq.ParquetWriter(path, table.schema, compression='gzip', mode='a') as writer:
               writer.write_table(table)

    print('Insertion complete')
    return path
    
    # while True:
    #     try:
    #        df = next(df_first)
    #        df.pickup_datetime = pd.to_datetime(df.pickup_datetime)
    #        df.dropOff_datetime = pd.to_datetime(df.dropOff_datetime)
    #        print(f'Inserted another chunk')
    #     except StopIteration:
    #         print(f'Finished ingesting the data')
    #         break
    # df.pickup_datetime = pd.to_datetime(df.pickup_datetime)
    # df.dropOff_datetime = pd.to_datetime(df.dropOff_datetime)

    # print(df.head(2))
    # print(f"columns: {df.dtypes}")
    # print(f"rows: {len(df)}")
    # return df
    


# @task()
# def write_local(df: pd.DataFrame, month: int) -> Path:
#     """Write DataFrame out locally as parquet file"""
#     path = Path(f"data/2019/fhv_{month:02}.parquet")
#     df.to_parquet(path, compression="gzip")
#     return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("my-nyc-taxi-bucket")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def new_web_to_gcs() -> None:
    """The main ETL function"""
    month = 1
    path = Path(f"data/2019/fhv_{month:02}.parquet")
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-{month:02}.csv.gz"
    

    df_chunks = fetch(dataset_url)
    path = loop_clean(df_chunks, path)
    write_gcs(path)


if __name__ == "__main__":
    new_web_to_gcs()