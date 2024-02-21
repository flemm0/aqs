from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator

from tasks.cbsa import get_cbsa_codes, write_cbsa_parquet_data
from tasks.monitors import get_monitors_by_cbsa

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 19),
    #'end_date': datetime(2024, 1, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'cbsa_codes',
    default_args=default_args,
    schedule_interval=timedelta(days=1)
)

# Define DAG tasks
download_cbsa_info_from_api_task = PythonOperator(
    task_id='download_cbsa_info_from_api_task',
    python_callable=get_cbsa_codes,
    provide_context=True,
    dag=dag,
)

write_cbsa_codes_to_disk_task = PythonOperator(
    task_id='write_cbsa_codes_to_disk_task',
    python_callable=write_cbsa_parquet_data,
    # op_kwargs={'cbsa_data': "{{ ti.xcom_pull(task_ids='fetch_cbsa_codes_from_api') }}"},
    provide_context=True,
    dag=dag,
)

date = "{{ ds_nodash }}"
write_monitor_by_cbsa_data_to_disk_task = PythonOperator(
    task_id='write_monitor_by_cbsa_data_to_disk_task',
    python_callable=get_monitors_by_cbsa,
    provide_context=True,
    op_kwargs={'date': date},
    dag=dag
)

# create_object = S3CreateObjectOperator(
#     task_id="create_object",
#     s3_bucket=bucket_name,
#     s3_key=key,
#     data=download_cbsa_info_from_api_task.output,
#     replace=True,
# )

ready = EmptyOperator(task_id='ready')

# Define task dependencies
download_cbsa_info_from_api_task >> [write_cbsa_codes_to_disk_task, write_monitor_by_cbsa_data_to_disk_task] >> ready