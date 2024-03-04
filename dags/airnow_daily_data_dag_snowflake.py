from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

from plugins.callables.airnow import *
from plugins.callables.sql import create_snowflake_tables_if_not_existing, insert_hourly_data_to_snowflake

from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig





# cosmos setup
profile_config = ProfileConfig(
    profile_name='airnow_aqs', 
    target_name='prod',
    profiles_yml_filepath='/home/airflow/.dbt/profiles.yml'
)


with DAG(
    'airnow_daily_data_snowflake',
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
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
    # start_date=datetime(2024, 1, 1),  # backfill starting from Jan 1, 2024
    end_date=days_ago(4, hour=23),
    schedule_interval='0 0 * * *', # daily at midnight
    tags=['airnow'],
    max_active_runs=1, # prevent OOM issues when Docker container hosted on local
    catchup=True
) as dag:
    
    create_staging_tables_if_not_existing_task = PythonOperator(
        task_id='create_staging_tables_if_not_existing_task',
        python_callable=create_snowflake_tables_if_not_existing,
        provide_context=True
    )

    with TaskGroup(group_id='hourly_data_task_group') as tg1:

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

        load_hourly_data_to_snowflake_task = PythonOperator(
            task_id='load_hourly_data_to_snowflake_task',
            python_callable=insert_hourly_data_to_snowflake,
            provide_context=True,
            dag=dag
        )

        cleanup_local_hourly_data_files_task = BashOperator(
            task_id='cleanup_local_hourly_data_files_task',
            bash_command='rm /opt/airflow/{{ ti.xcom_pull(task_ids="hourly_data_task_group.extract_aqobs_daily_data_task", key="aqobs_data") }}',
            dag=dag
        )

        extract_aqobs_daily_data_task >> write_daily_aqobs_data_to_s3_task >> load_hourly_data_to_snowflake_task >> cleanup_local_hourly_data_files_task



    done = EmptyOperator(task_id='done')


    create_staging_tables_if_not_existing_task >> tg1 >> done