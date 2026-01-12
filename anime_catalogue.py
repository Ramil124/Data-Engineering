from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
import requests
import pandas as pd
import re



## setup for the Dag
default_args = {
    'owner':'r124',
    'start_date':datetime(2026,1,11),
    'retries':1,
    'retry_delay':timedelta(minutes=2)
}


## Extract data

def extract_function(**context):

    ## taking the url for the data 
    url = 'https://api.jikan.moe/v4/top/anime?sfw'
    response = requests.get(url)
    response.raise_for_status()

    ## Creating a pandas table to use the pandas functionality to get the columns we need
    data = response.json()['data']
    df = pd.DataFrame(data)
    columns = ['mal_id','title_english','aired', 'type', 'status', 'synopsis',
        'episodes', 'duration','rank','score','rating','broadcast']
    
    df= df[columns]

    ## broadcast had dict within dict so I have used lambda to only get the day of the week
    df['broadcast_weekday'] = df['broadcast'].apply(
        lambda x: x.get('day') if isinstance(x, dict) else None
    )
    ## same logic as above for the time of broadcasting
    df['broadcast_time'] = df['broadcast'].apply(
        lambda x: x.get('time') if isinstance(x, dict) else None
    )

    ## same logic as above for the date of when the show was aired
    df['from_date'] = df['aired'].apply(
        lambda x: x.get('from') if isinstance(x, dict) else None
    )
    ## pushig the json file
    context['ti'].xcom_push(key = 'extracted_data', value = df.to_json())
    
    return 'Extraction Complete'

    




## transform data


## the duration column was in a string format so i have cornverted it into minutes so it is easier to analyze the data
def duration_to_minutes(duration_str):
    hours = 0
    minutes = 0
    
    
## converting string of duration into minutes
    if not isinstance(duration_str, str):
        return None
    
## Extract hours
    h_match = re.search(r'(\d+)\s*hr', duration_str)
    if h_match:
        hours = int(h_match.group(1))
## Exctract minutes
    m_match = re.search(r'(\d+)\s*min', duration_str)

    if m_match:
        minutes = int(m_match.group(1))

    if hours == 0 and minutes == 0:
        return None

    return hours*60+minutes



def transform_function(**context):

## Pulling the data from the json that we have extracted for transformation
    json_data = context['ti'].xcom_pull(task_ids= 'extract_function', key = 'extracted_data')
    df = pd.read_json(json_data)
## converting strings for date columns into dates for easier analysis later
    df['from_date']= pd.to_datetime(df['from_date'], errors = 'coerce').dt.date
    df['duration_minutes'] = df['duration'].apply(duration_to_minutes)
    df['broadcast_time'] = df['broadcast_time'].fillna('')
    df['broadcast_weekday'] = df['broadcast_weekday'].replace('', None)
    df['from_date'] = df['from_date'].astype(str).replace('NaT', '')

    ## dropping the excessive columns
    df = df.drop(columns=['aired', 'broadcast','duration'], errors='ignore')

    df = df[['mal_id', 'title_english','type','status',
            'synopsis','episodes','duration_minutes','rank',
            'score','rating','broadcast_weekday','broadcast_time',
            'from_date']]
    temp_path = '/opt/airflow/data/anime_catalogue.csv'

     ## 1uoting = 1 is used to ensure titles do not break the csv
    df.to_csv(temp_path, index = False, quoting=1)
    context['ti'].xcom_push(key = 'transformed_path', value = temp_path)

    return 'Transformation complete'




## DAG
with DAG(dag_id = 'anime_catalogue_dag', 
         default_args=default_args, 
         start_date=datetime(2026,1,11),
         schedule='@daily', catchup=False) as dag:
    extract_task = PythonOperator(task_id = 'extract_function', 
                                  python_callable= extract_function)
    transform_task = PythonOperator(task_id = 'transform_task', 
                                    python_callable=transform_function)
    
## I have use BashOperator syntax to first create a table in postgres db
    create_table = BashOperator(
        task_id = 'create_anime_table',
        bash_command="""
    PGPASSWORD=airflow \
    psql -h postgres -p 5432 -U airflow -d airflow \
    -v ON_ERROR_STOP=1 \
    -f /opt/airflow/dags/sql/create_anime_table.sql
    """
    )
    

## then once again used the BashOperator to load the data into that table 
    load_to_postgres = BashOperator(task_id = 'load_to_postgres', 
                                    bash_command="""
    PGPASSWORD=airflow \
    psql -h postgres -p 5432 -U airflow -d airflow \
    -c "\\copy anime FROM '/opt/airflow/data/anime_catalogue.csv' CSV HEADER NULL ''"
    """)

## The chain of operations
extract_task >> transform_task >> create_table >> load_to_postgres
    
    


