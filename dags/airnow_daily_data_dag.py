from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.utils.dates import days_ago

import requests
from io import BytesIO

from config.constants import DATA_PATH
from config.schemas import hourly_aqobs_file_schema


with DAG(
    'airnow_daily_data',
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function, # or list of functions
        # 'on_success_callback': some_other_function, # or list of functions
        # 'on_retry_callback': another_function, # or list of functions
        # 'sla_miss_callback': yet_another_function, # or list of functions
        # 'trigger_rule': 'all_success'
    },
    description='Extracts hourly-updated air quality measurements from Airnow API every day',
    start_date=days_ago(4), #data is updated continuously for 48 hours after posting
    end_date=days_ago(4, hour=23),
    schedule_interval='0 0 * * *', # daily at midnight
    catchup=False,
    tags=['airnow'],
) as dag:

    def extract_aqobs_daily_data(date: str):
        import polars as pl

        data = []
        for num in ["{:02d}".format(i) for i in range(0, 24)]:
            url = f'https://s3-us-west-1.amazonaws.com/files.airnowtech.org/airnow/{date[:4]}/{date}/HourlyAQObs_{date}{num}.dat'
            print(f'Extracting file: HourlyAQObs_{date}{num}.dat')
            response = requests.get(url)
            df = pl.read_csv(BytesIO(response.content), ignore_errors=True, schema=hourly_aqobs_file_schema)
            data.extend(df.to_dicts())
        
        df = pl.DataFrame(data, schema=hourly_aqobs_file_schema)

        path = DATA_PATH / 'hourly_data' / date[:4]
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)

        df.write_parquet(f'{path}/{date}.parquet')
        

    extract_aqobs_daily_data_task = PythonOperator(
        task_id='extract_aqobs_daily_data_task',
        python_callable=extract_aqobs_daily_data,
        op_kwargs={'date': "{{ macros.ds_format(ds, '%Y-%m-%d', '%Y%m%d') }}"},
        provide_context=True
    )

    ready = EmptyOperator(task_id='ready')

    extract_aqobs_daily_data_task >> ready