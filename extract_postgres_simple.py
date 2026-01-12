from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

def extract_from_postgres():
    hook = PostgresHook(postgres_conn_id = 'pg_first_conn_id')
    df = hook.get_pandas_df(sql="SELECT * FROM dag_code;")
    df.to_csv("/opt/airflow/data/dag_code.csv", index=False)

def transform_from_postgres():
    df = pd.read_csv('/opt/airflow/data/dag_code.csv')
    df_transformed = df.loc[df['dag_id'].astype(str).str.contains('dag')]
    df_transformed.to_csv("/opt/airflow/data/dag_code_transformed.csv", index = False)

default_args = {
    'owner':'r124',
    'start_date':datetime(2025,11, 17),
    'retries':5,
    'retry_delay':timedelta(minutes=2)
}


with DAG('first_pg_extract', default_args=default_args,catchup = False,  schedule='@daily') as dag:
    extract_task = PythonOperator(
        task_id = 'extract_from_postgres',
        python_callable=extract_from_postgres
    )

    transform_task = PythonOperator(
        task_id = 'transform_from_postgres',
        python_callable= transform_from_postgres
    )
extract_task >>  transform_task