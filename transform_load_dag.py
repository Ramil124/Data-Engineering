## importing libraries 
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
import psycopg2

## settings for the Dag
default_args = {
    'owner':'r124',
    'start_datetime':datetime(2025, 11, 18),
    'retries':5,
    'retry_delay':timedelta(minutes = 2)
}

## pulling the data extracted by bash command
def transforming_fn(ti):
    path = ti.xcom_pull(task_ids = 'extracting')
    df = pd.read_csv(path)
    ## selecting the columns necessary
    df = df[['VendorID','lpep_pickup_datetime','lpep_dropoff_datetime','passenger_count','trip_distance','total_amount']]
    ## converting dates
    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])

    ## making sure that we get the data valid values
    df = df.loc[(df['trip_distance'] > 0) & (df['passenger_count'] > 0)]
    temp_path = '/opt/airflow/data/green_temp.csv'
    df.to_csv(temp_path, index = False)

    ## pushing the table 
    ti.xcom_push(key = 'transformed_path', value = temp_path)


## loading function that uses python syntax to load the final table into postgres
def loading_into_pg(ti):
    path = ti.xcom_pull(task_ids= 'transforming', key = 'transformed_path')
    df = pd.read_csv(path)
    engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres:5432/airflow")
    df.to_sql('green_transformed', engine, if_exists = 'replace', index = False)






with DAG(dag_id = 'load_to_postgres_id', default_args=default_args, schedule='@daily', catchup=False) as dag:

    ## Using bash operator commands for easier and quicker extraction of the data
    extract_task = BashOperator(
        task_id = 'extracting',
        bash_command= "wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz -O /opt/airflow/data/green_temp.csv.gz && \
                    gunzip -f /opt/airflow/data/green_temp.csv.gz && \
                    echo /opt/airflow/data/green_temp.csv",
        do_xcom_push = True
    )

    transform_task = PythonOperator(
        task_id = 'transforming',
        python_callable= transforming_fn
    )

    load_task = PythonOperator(
        task_id = 'loading',
        python_callable=loading_into_pg
    )

## operation chain sequence
    extract_task >> transform_task >> load_task
