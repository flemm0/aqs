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
from plugins.callables.sql import create_md_staging_tables_if_not_existing
from plugins.operators.s3 import S3ToMotherDuckInsertOperator, S3ToMotherDuckInsertNewRowsOperator

from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig





# cosmos setup
profile_config = ProfileConfig(
    profile_name='airnow_aqs', 
    target_name='dev',
    profiles_yml_filepath='/home/airflow/.dbt/profiles.yml'
)


with DAG(
    'airnow_daily_data_duckdb',
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
    # start_date=days_ago(4), # data is updated continuously for 48 hours after posting
    start_date=datetime(2024, 1, 1),  # backfill starting from Jan 1, 2024
    end_date=days_ago(4, hour=23),
    schedule_interval='0 0 * * *', # daily at midnight
    tags=['airnow'],
    max_active_runs=1, # prevent OOM issues when Docker container hosted on local
    catchup=True
) as dag:

    create_staging_tables_if_not_existing_task = PythonOperator(
        task_id='create_staging_tables_if_not_existing_task',
        python_callable=create_md_staging_tables_if_not_existing,
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

        load_hourly_data_to_motherduck_task = S3ToMotherDuckInsertOperator(
            task_id='load_hourly_data_to_motherduck_task',
            s3_bucket='airnow-aq-data-lake',
            s3_key="{{ ti.xcom_pull(task_ids='hourly_data_task_group.write_daily_aqobs_data_to_s3_task', key='aqobs_s3_object_path') }}",
            table='staging.stg_hourly_data',
            dag=dag
        )
        
        cleanup_local_hourly_data_files_task = BashOperator(
            task_id='cleanup_local_hourly_data_files_task',
            bash_command='rm /opt/airflow/{{ ti.xcom_pull(task_ids="hourly_data_task_group.extract_aqobs_daily_data_task", key="aqobs_data") }}',
            dag=dag
        )

        extract_aqobs_daily_data_task >> write_daily_aqobs_data_to_s3_task >>\
              load_hourly_data_to_motherduck_task >> cleanup_local_hourly_data_files_task
        
    with TaskGroup(group_id='reporting_areas_task_group') as tg2:

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
            s3_key="{{ ti.xcom_pull(task_ids='reporting_areas_task_group.write_reporting_area_locations_to_s3_task', key='reporting_areas_s3_object_path') }}",
            table='staging.stg_reporting_areas',
            temp_table='staging.temp_stg_reporting_areas',
            date="{{ ds }}",
            dag=dag
        )

        cleanup_reporting_area_location_files_task = BashOperator(
            task_id='cleanup_reporting_area_location_files_task',
            bash_command="rm /opt/airflow/{{ ti.xcom_pull(task_ids='reporting_areas_task_group.extract_reporting_area_locations_task', key='reporting_area_locations') }}",
            dag=dag
        )

        extract_reporting_area_locations_task >> write_reporting_area_locations_to_s3_task >> \
            load_reporting_area_locations_new_data_to_motherduck_task >> cleanup_reporting_area_location_files_task
            

    with TaskGroup(group_id='monitoring_sites_task_group') as tg3:

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
            s3_key="{{ ti.xcom_pull(task_ids='monitoring_sites_task_group.write_monitoring_site_locations_to_s3_task', key='monitoring_sites_s3_object_path') }}",
            table='staging.stg_monitoring_sites',
            temp_table='staging.temp_stg_monitoring_sites',
            date="{{ ds }}",
            dag=dag
        )

        cleanup_monitoring_site_location_files_task = BashOperator(
            task_id='cleanup_monitoring_site_location_files_task',
            bash_command="rm /opt/airflow/{{ ti.xcom_pull(task_ids='monitoring_sites_task_group.extract_monitoring_site_locations_task', key='monitoring_site_locations') }}",
            dag=dag
        )

        extract_monitoring_site_locations_task >> write_monitoring_site_locations_to_s3_task >> \
            load_monitoring_site_locations_new_data_to_motherduck_task >> cleanup_monitoring_site_location_files_task
        
    
    with TaskGroup(group_id='monitoring_sites_to_reporting_areas_task_group') as tg4:

        extract_monitoring_sites_to_reporting_areas_task = PythonOperator(
            task_id='extract_monitoring_sites_to_reporting_areas_task',
            python_callable=extract_monitoring_sites_to_reporting_areas,
            op_kwargs={'date': "{{ macros.ds_format(ds, '%Y-%m-%d', '%Y%m%d') }}"},
            provide_context=True
        )

        write_monitoring_sites_to_reporting_areas_to_s3_task = PythonOperator(
            task_id='write_monitoring_sites_to_reporting_areas_to_s3_task',
            python_callable=write_monitoring_sites_to_reporting_areas_to_s3,
            provide_context=True
        )

        load_monitoring_sites_to_reporting_areas_to_motherduck_task = S3ToMotherDuckInsertNewRowsOperator(
            task_id='load_monitoring_sites_to_reporting_areas_to_motherduck_task',
            s3_bucket='airnow-aq-data-lake',
            s3_key="{{ ti.xcom_pull(task_ids='monitoring_sites_to_reporting_areas_task_group.write_monitoring_sites_to_reporting_areas_to_s3_task', key='monitoring_sites_to_reporting_areas_s3_object_path') }}",
            table='staging.stg_monitoring_sites_to_reporting_areas',
            temp_table='staging.temp_stg_monitoring_sites_to_reporting_areas',
            date="{{ ds }}",
            dag=dag
        )

        cleanup_monitoring_sites_to_reporting_areas_files_task = BashOperator(
            task_id='cleanup_monitoring_sites_to_reporting_areas_files_task',
            bash_command="rm /opt/airflow/{{ ti.xcom_pull(task_ids='monitoring_sites_to_reporting_areas_task_group.extract_monitoring_sites_to_reporting_areas_task', key='monitoring_sites_to_reporting_areas') }}",
            dag=dag
        )

        extract_monitoring_sites_to_reporting_areas_task >> write_monitoring_sites_to_reporting_areas_to_s3_task >> \
            load_monitoring_sites_to_reporting_areas_to_motherduck_task >> cleanup_monitoring_sites_to_reporting_areas_files_task


    dbt_task_group = DbtTaskGroup(
        project_config=ProjectConfig(
        dbt_project_path=f'{os.environ["AIRFLOW_HOME"]}/dags/dbt/airnow_aqs',
        env_vars={'HOME': '/home/airflow'}
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path='/home/airflow/.local/bin/dbt',
        ),
    )


    done = EmptyOperator(task_id='done')


    # execution order definitions

    create_staging_tables_if_not_existing_task >> tg1 >> [tg2, tg3] >> tg4 >> dbt_task_group >> done