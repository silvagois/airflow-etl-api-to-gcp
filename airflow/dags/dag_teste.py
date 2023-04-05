from airflow import DAG
from datetime import datetime
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

def print_hello():
    print('Testando Dag')


with DAG(
    'dag_teste', 
     #start_date = datetime(2023,4,4),
     start_date=days_ago(1),
     schedule_interval = '1 * * * *',
     tags=['pipeline'] ,
     catchup=False) as dag:
    
    print_hello = PythonOperator(
        task_id = 'print_hello',
        python_callable = print_hello
    )

print_hello