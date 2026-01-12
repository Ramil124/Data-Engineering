from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator


default_args = {
    'owner':'r124',
    'standard_datetime':datetime(2026, 1, 11), 
    'retries':5,
    'retry_delay':timedelta(minutes=2)
}


with DAG(dag_id='dag_catchup_backfill_v02',
          default_args=default_args, 
          schedule='@daily',
            catchup=False,
              start_date=datetime(2025, 1, 1)) as dag:
    task1 = BashOperator(task_id = 'task1',
                         bash_command='echo This is a simple bash command!')