from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.extract import weather_api
from src.transform import transform_weather
from src.load import load_weather
import sys

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

cities = ['Kuźnia Raciborska', 'Racibórz', 'Warszawa', 'Toruń']

with DAG(
        'weather_etl',
        default_args=default_args,
        description='Weather ETL',
        schedule_interval='@hourly',
        catchup=False,
) as dag:
    extract_task = PythonOperator(
        task_id='extract_weather',
        python_callable=weather_api.get_weather_data,
        op_args=[cities]
    )

    transform_task = PythonOperator(
        task_id='transform_weather',
        python_callable=transform_weather.transform_batch,
        op_args=[extract_task.output],
    )

    load_task = PythonOperator(
        task_id='load_weather',
        python_callable=load_weather.load_weather_data,
        op_args=[transform_task.output],
    )

    extract_task >> transform_task >> load_task