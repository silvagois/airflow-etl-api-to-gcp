from airflow import DAG
#from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from utils import extract_data_from_api, transform_data, load_local_data


with DAG(
    'etl_api_to_gcp', 
     #start_date = datetime(2023,4,4),
     start_date=days_ago(1),
     schedule_interval = '1 * * * *',
     tags=['pipeline'] ,     
     catchup=False) as dag:
    
    extract_data_from_api = PythonOperator(
        task_id = 'extract_data_from_api',
        python_callable = extract_data_from_api
    )
    transform_data = PythonOperator(
        task_id = 'transform_data',
        python_callable = transform_data
    )

    load_local_data = PythonOperator(
        task_id = 'load_local_data',
        python_callable = load_local_data
    )
extract_data_from_api >> transform_data >> load_local_data