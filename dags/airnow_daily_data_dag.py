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
    description='A simple tutorial DAG',
    start_date=days_ago(3), #data is updated continuously for 48 hours after posting
    schedule_interval='@daily',
    catchup=False,
    tags=['airnow'],
) as dag:
    
    def extract_daily_monitor_data(date: str):
        import polars as pl

        for num in ["{:02d}".format(i) for i in range(0, 24)]:
            url = f'https://s3-us-west-1.amazonaws.com/files.airnowtech.org/airnow/{date[:4]}/{date}/HourlyData_{date}{num}.dat'
            response = requests.get(url)
            df = pl.read_csv(BytesIO(response.content), separator='|', ignore_errors=True)
            print(df)

    extract_daily_monitor_data_task = PythonOperator(
        task_id='extract_daily_monitor_data_task',
        python_callable=extract_daily_monitor_data,
        op_kwargs={'date': "{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%Y%m%d') }}"},
        provide_context=True
    )

    ready = EmptyOperator(task_id='ready')

    extract_daily_monitor_data_task >> ready

    # download_from_server_file_storage_task = BashOperator(
    #     task_id='download_from_server_file_storage_task',
    #     bash_command='''
    #     curl -o - 'https://s3-us-west-1.amazonaws.com/files.airnowtech.org/airnow/{{ ds_nodash[:4] }}/{{ ds_nodash }}/HourlyData_{{ ds_nodash }}00.dat'
    #     '''.strip(),
    #     xcom_push=True
    # )

    # def write_hourly_monitor_data_to_disk(**kwargs):
    #     import polars as pl

    #     ti = kwargs['ti']
    #     data = ti.xcom_pull(task_ids='download_from_server_file_storage_task')

    #     print(data)
    #     # df = pl.read_csv(data, separator='|')
    #     # print(df)

    # write_hourly_monitor_data_to_disk_task = PythonOperator(
    #     task_id='write_hourly_monitor_data_to_disk_task',
    #     python_callable=write_hourly_monitor_data_to_disk,
    #     provide_context=True
    # )

    # download_from_server_file_storage_task >> write_hourly_monitor_data_to_disk_task