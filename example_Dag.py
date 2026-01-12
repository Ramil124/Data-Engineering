from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import pandas as pd
import clickhouse_connect
import psycopg2

def extract_from_postgres():
    # Получаем параметры подключения из Airflow Connections
    conn = BaseHook.get_connection('postgres_conn_id')
    conn_uri = f"host={conn.host} dbname={conn.schema} user={conn.login} password={conn.password} port={conn.port}"
    # SQL-запрос
    query = "SELECT * FROM my_table;"
    # Загружаем в pandas DataFrame
    with psycopg2.connect(conn_uri) as pg_conn:
        df = pd.read_sql(query, pg_conn)
    # Сохраняем во временный CSV (или передай через XCom)
    df.to_csv('/tmp/my_table.csv', index=False)

def load_to_clickhouse():
    # Получаем параметры подключения
    conn = BaseHook.get_connection('clickhouse_conn_id')
    client = clickhouse_connect.get_client(
        host=conn.host,
        port=conn.port or 8123,
        username=conn.login,
        password=conn.password,
        database=conn.schema
    )
    # Читаем из CSV
    df = pd.read_csv('/tmp/my_table.csv')
    # Загружаем в ClickHouse
    client.insert_df('my_table', df)

default_args = {
    'start_date': datetime(2024, 1, 1),
    'catchup': False
}

with DAG(
    'pg_to_clickhouse',
    default_args=default_args,
    schedule='@daily',
) as dag:
    extract = PythonOperator(
        task_id='extract_from_postgres',
        python_callable=extract_from_postgres
    )

    load = PythonOperator(
        task_id='load_to_clickhouse',
        python_callable=load_to_clickhouse
    )

    extract >> load






