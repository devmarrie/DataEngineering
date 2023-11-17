from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

def hello():
    print("Hello from airflow")

default_args = {
    'owner': 'your_name',
    'start_date': datetime(2023, 11, 2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('airflow_tutorial',
         default_args=default_args,
         schedule_interval='0 * * * *'
         ) as dag:
    print_hello = BashOperator(task_id='Hello example',
                               bash_command='echo "hello I will sleep soon"')
    sleep = BashOperator(task_id='Sleep a little',
                         bash_command='sleep 5')
    print_world = PythonOperator(task_id='print_hello_world',
                                 python_callable=hello)
    
print_hello >> sleep >> print_world