from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

from plugins.callables.airnow import *
from plugins.operators.s3 import S3ToMotherDuckInsertOperator, S3ToMotherDuckInsertNewRowsOperator

with DAG(
    'airnow_daily_data',
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
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
    start_date=days_ago(4), # data is updated continuously for 48 hours after posting
    # start_date=days_ago(30), # backfill starting from 30 days ago
    end_date=days_ago(4, hour=23),
    schedule_interval='0 0 * * *', # daily at midnight
    tags=['airnow'],
    max_active_runs=1, # prevent OOM issues when hosted on local
    catchup=True
) as dag:

    extract_aqobs_daily_data_task = PythonOperator(
        task_id='extract_aqobs_daily_data_task',
        python_callable=extract_aqobs_daily_data,
        op_kwargs={'date': "{{ macros.ds_format(ds, '%Y-%m-%d', '%Y%m%d') }}"},
        provide_context=True
    )

    write_daily_aqobs_data_to_s3_task = PythonOperator(
        python_callable=write_daily_aqobs_data_to_s3,
        task_id='write_daily_aqobs_data_to_s3_task',
        provide_context=True
    )

    load_hourly_data_to_motherduck_task = S3ToMotherDuckInsertOperator(
        task_id='load_hourly_data_to_motherduck_task',
        s3_bucket='airnow-aq-data-lake',
        s3_key="{{ ti.xcom_pull(task_ids='write_daily_aqobs_data_to_s3') }}",
        table='staging.stg_hourly_data',
        dag=dag
    )

    cleanup_local_hourly_data_files_task = PythonOperator(
        task_id='cleanup_local_hourly_data_files_task',
        python_callable=cleanup_local_hourly_data_files,
        provide_context=True
    )

    extract_reporting_area_locations_task = PythonOperator(
        task_id='extract_reporting_area_locations_task',
        python_callable=extract_reporting_area_locations,
        op_kwargs={'date': "{{ macros.ds_format(ds, '%Y-%m-%d', '%Y%m%d') }}"},
        provide_context=True
    )

    write_reporting_area_locations_to_s3_task = PythonOperator(
        python_callable=write_reporting_area_locations_to_s3,
        task_id='write_reporting_area_locations_to_s3_task',
        provide_context=True
    )

    load_reporting_area_locations_new_data_to_motherduck_task = S3ToMotherDuckInsertNewRowsOperator(
        task_id='load_reporting_area_locations_new_data_to_motherduck_task',
        s3_bucket='airnow-aq-data-lake',
        s3_key="{{ ti.xcom_pull(task_ids='write_reporting_area_locations_to_s3_task') }}",
        table='staging.stg_reporting_areas',
        temp_table='staging.temp_stg_reporting_areas'
    )

    cleanup_reporting_area_location_files_task = PythonOperator(
        task_id='cleanup_reporting_area_location_files_task',
        python_callable=cleanup_reporting_area_location_files,
        provide_context=True
    )

    extract_monitoring_site_locations_task = PythonOperator(
        task_id='extract_monitoring_site_locations_task',
        python_callable=extract_monitoring_site_locations,
        op_kwargs={'date': "{{ macros.ds_format(ds, '%Y-%m-%d', '%Y%m%d') }}"},
        provide_context=True
    )

    write_monitoring_site_locations_to_s3_task = PythonOperator(
        python_callable=write_monitoring_site_locations_to_s3,
        task_id='write_monitoring_site_locations_to_s3_task',
        provide_context=True
    )

    load_monitoring_site_locations_new_data_to_motherduck_task = S3ToMotherDuckInsertNewRowsOperator(
        task_id='load_monitoring_site_locations_new_data_to_motherduck_task',
        s3_bucket='airnow-aq-data-lake',
        s3_key="{{ ti.xcom_pull(task_ids='write_monitoring_site_locations_to_s3_task') }}",
        table='staging.stg_monitoring_sites',
        temp_table='staging.temp_stg_monitoring_sites'
    )

    cleanup_monitoring_site_location_files_task = PythonOperator(
        task_id='cleanup_monitoring_site_location_files_task',
        python_callable=cleanup_monitoring_site_location_files,
        provide_context=True
    )


    done = EmptyOperator(task_id='done')


    
    extract_aqobs_daily_data_task >> write_daily_aqobs_data_to_s3_task >> load_hourly_data_to_motherduck_task >> cleanup_local_hourly_data_files_task
    cleanup_local_hourly_data_files_task >> [extract_reporting_area_locations_task, extract_monitoring_site_locations_task]
    extract_monitoring_site_locations_task >> write_monitoring_site_locations_to_s3_task >> load_monitoring_site_locations_new_data_to_motherduck_task >> cleanup_monitoring_site_location_files_task
    write_reporting_area_locations_to_s3_task >> load_reporting_area_locations_new_data_to_motherduck_task >> load_reporting_area_locations_new_data_to_motherduck_task >> cleanup_reporting_area_location_files_task
    [cleanup_monitoring_site_location_files_task, cleanup_reporting_area_location_files_task] >> done