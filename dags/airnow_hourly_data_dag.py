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
from config.schemas import hourly_aqobs_file_schema, reporting_area_locations_v2_schema, monitoring_site_location_v2_schema


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
    
    def extract_aqobs_daily_data(today: str, yesterday: str):
        import polars as pl

        # update starting from yesterday
        for hour in ["{:02d}".format(i) for i in range(0, 24)]:
            url = f'https://s3-us-west-1.amazonaws.com/files.airnowtech.org/airnow/today/HourlyAQObs_{yesterday}{hour}.dat'
            response = requests.get(url)
            if response.status_code == 200:
                df = pl.read_csv(
                    BytesIO(response.content), 
                    separator='|', 
                    ignore_errors=True, 
                    schema=hourly_aqobs_file_schema
                )

                path = DATA_PATH / 'hourly_data' / yesterday[:4]
                if not path.exists():
                    path.mkdir(parents=True, exist_ok=True)

                df.write_parquet(f'{path}/{yesterday}.parquet')

        # update today
        for hour in ["{:02d}".format(i) for i in range(0, 24)]:
            url = f'https://s3-us-west-1.amazonaws.com/files.airnowtech.org/airnow/today/HourlyAQObs_{today}{hour}.dat'
            response = requests.get(url)
            if response.status_code == 200:
                df = pl.read_csv(
                    BytesIO(response.content), 
                    separator='|', 
                    ignore_errors=True, 
                    schema=hourly_aqobs_file_schema
                )

                path = DATA_PATH / 'hourly_data' / today[:4]
                if not path.exists():
                    path.mkdir(parents=True, exist_ok=True)

                df.write_parquet(f'{path}/{today}.parquet')
        

    extract_aqobs_daily_data_task = PythonOperator(
        task_id='extract_aqobs_daily_data_task',
        python_callable=extract_aqobs_daily_data,
        op_kwargs={
            'today': "{{ macros.ds_format(ds, '%Y-%m-%d', '%Y%m%d') }}",
            'yesterday': "{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%Y%m%d') }}",
        },
        provide_context=True
    )


    def extract_reporting_area_locations(today: str):
        '''Extracts data files provided on the reporting areas'''
        import polars as pl

        url = f'https://s3-us-west-1.amazonaws.com/files.airnowtech.org/airnow/today/reporting_area_locations_V2.dat'
        print(f'Extracting file: reporting_area_locations_V2.dat')
        response = requests.get(url)
        if response.status_code == 200:
            df = pl.read_csv(
                BytesIO(response.content),
                ignore_errors=True,
                separator='|',
                schema=reporting_area_locations_v2_schema
            )

            path = DATA_PATH / 'reporting_areas' / today[:4]
            if not path.exists():
                path.mkdir(parents=True, exist_ok=True)

            df.write_parquet(f'{path}/{today}.parquet')

    extract_reporting_area_locations_task = PythonOperator(
        task_id='extract_reporting_area_locations_task',
        python_callable=extract_reporting_area_locations,
        op_kwargs={'today': "{{ macros.ds_format(ds, '%Y-%m-%d', '%Y%m%d') }}"},
        provide_context=True
    )


    def extract_monitoring_site_locations(today: str):
        '''Extracts data files provided on monitorinig sites'''
        import polars as pl

        url = f'https://s3-us-west-1.amazonaws.com//files.airnowtech.org/airnow/today/Monitoring_Site_Locations_V2.dat'
        print(f'Extracting file: Monitoring_Site_Locations_V2.dat')
        response = requests.get(url)
        if response.status_code == 200:
            df = pl.read_csv(
                BytesIO(response.content),
                ignore_errors=True,
                separator='|',
                schema=monitoring_site_location_v2_schema
            )

            path = DATA_PATH / 'monitoring_sites' / today[:4]
            if not path.exists():
                path.mkdir(parents=True, exist_ok=True)

            df.write_parquet(f'{path}/{today}.parquet')

    extract_monitoring_site_locations_task = PythonOperator(
        task_id='extract_monitoring_site_locations_task',
        python_callable=extract_monitoring_site_locations,
        op_kwargs={'today': "{{ macros.ds_format(ds, '%Y-%m-%d', '%Y%m%d') }}"},
        provide_context=True
    )

    ready = EmptyOperator(task_id='ready')

    extract_aqobs_daily_data_task >> [extract_reporting_area_locations_task, extract_monitoring_site_locations_task] >> ready