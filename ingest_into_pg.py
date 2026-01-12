from airflow import DAG
from datetime import datetime
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
from sqlalchemy import create_engine
import pandas as pd


def extract_and_load_postgres(**kwargs):
    engine = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"

    base_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green"
    months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
    year = '2019'
    urls = [f"{base_url}/green_tripdata_{year}-{month}.csv.gz" for month in months]

    total_rows = 0
    for url in urls:
        for chunk in pd.read_csv(url, compression='gzip', chunksize=100000):
            chunk.to_sql('green_tripdata', engine, index = False, if_exists='append')
            total_rows +=len(chunk)
            print(f"Loaded {len(chunk)} rows from {url}")
    
    kwargs['ti'].xcom_push(key = "total_rows_loaded", value = total_rows)
    print(f"Total rows loaded {total_rows}")

default_args = {'start_date':datetime(2025, 11, 17)}

with DAG(dag_id='nyc_green_extract_load', schedule = None,
         default_args=default_args,catchup=False ) as dag:
    extract_load_task = PythonOperator(
        task_id = 'extract_and_load_postgres',
        python_callable=extract_and_load_postgres
    )
