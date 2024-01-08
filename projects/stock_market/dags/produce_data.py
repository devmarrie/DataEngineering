from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer
import json



default_args = {
    'owner': 'stocks',
    'start_date': datetime(2024,1,1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def produce_stream():
    df = pd.read_csv('data/TSLA_20-24.csv')
    record = df.sample(1).to_dict(orient="records")[0]
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    producer.send('daily_stock_prices', json.dumps(record).encode('utf-8'))


                                                                                                                                                                                                                                                                                                                

with DAG('produce_stock_data',
         default_args=default_args,
         schedule='@daily',
         catchup= False
) as dag:

    task_produce_stream = PythonOperator(
        task_id='produce_stream',
        python_callable=produce_stream
    )

# produce_stream()
