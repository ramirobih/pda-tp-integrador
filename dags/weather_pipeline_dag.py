from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Añadir el directorio de scripts al path
#sys.path.insert(0,os.path.join(os.path.dirname(os.path.abspath(__file__)), '../scripts'))

# Importar la función main del script weather_extractor.py
#
from scripts.weather_extractor import main as weather_pipeline

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'email': ['tu_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'weather_pipeline',
    default_args=default_args,
    description='Pipeline para extraer datos meteorológicos y cargarlos en Redshift',
    schedule_interval='@hourly',  # Ejecutar una vez por hora
    catchup=False,
) as dag:

    run_pipeline = PythonOperator(
        task_id='run_weather_pipeline',
        python_callable=weather_pipeline,
    )