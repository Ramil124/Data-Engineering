from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner':'r124',
    'retries':5,
    'retry_delay':timedelta(minutes = 2),
    'start_date':datetime(2025, 11, 18)
}

def process_file(ti):
    path = ti.xcom_pull(task_ids = "using_bash")
    df = pd.read_csv(path)
    row_count = len(df)
    ti.xcom_push(key = 'total_rows_loaded', value = row_count)


with DAG(dag_id = 'bash_dag', default_args=default_args, schedule='@daily', catchup=False) as dag:
    bash_task = BashOperator(
        task_id = 'using_bash',

        ## wget -> file downloaded -> gunzip -> CSV extracted -> echo -> XCom

        ## -f replaces the file without asking first if it exists
        # echo Print the path of the extracted CSV so Airflow captures it
        # && Only run the next command if the previous one succeeded
        # -O  output flag, tells wget where to save the downloaded file.
        bash_command="wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz -O /opt/airflow/data/green_using_bash.csv.gz && \
                    gunzip -f /opt/airflow/data/green_using_bash.csv.gz && \
                    echo /opt/airflow/data/green_using_bash.csv",
        do_xcom_push = True
    )

    python_task = PythonOperator(
        task_id = 'python_task_id',
        python_callable=process_file
    )

    bash_task >> python_task