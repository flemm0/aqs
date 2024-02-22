from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.utils.dates import days_ago

import requests
from io import BytesIO


with DAG(
    'airnow_hourly_data',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function, # or list of functions
        # 'on_success_callback': some_other_function, # or list of functions
        # 'on_retry_callback': another_function, # or list of functions
        # 'sla_miss_callback': yet_another_function, # or list of functions
        # 'trigger_rule': 'all_success'
    },
    description='Extracts air quality measurements from Airnow API every hour',
    start_date=days_ago(0),
    schedule_interval='@hourly',
    catchup=False,
    tags=['airnow'],
) as dag:
    
    def extract_hourly_monitor_data(date: str, timestamp_str: str):
        import polars as pl

        timestamp = datetime.fromisoformat(timestamp_str)
        hour = timestamp.hour

        url = f'https://s3-us-west-1.amazonaws.com/files.airnowtech.org/airnow/{date[:4]}/{date}/HourlyData_{date}{hour}.dat'
        response = requests.get(url)
        df = pl.read_csv(BytesIO(response.content), separator='|', ignore_errors=True)
        print(df)

    extract_hourly_monitor_data_task = PythonOperator(
        task_id='extract_hourly_monitor_data_task',
        python_callable=extract_hourly_monitor_data,
        op_kwargs={
            'date': "{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%Y%m%d') }}",
            'timestamp_str': "{{ ts }}"
        },
        provide_context=True
    )

    ready = EmptyOperator(task_id='ready')

    extract_hourly_monitor_data_task >> ready