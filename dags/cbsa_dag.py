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
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'cbsa_codes',
    default_args=default_args,
    schedule_interval=timedelta(days=1)
)

# Define DAG tasks
download_cbsa_info_from_api_task = PythonOperator(
    task_id='fetch_cbsa_codes_from_api',
    python_callable=get_cbsa_codes,
    dag=dag,
)

write_cbsa_codes_to_disk_task = PythonOperator(
    task_id='write_cbsa_codes_parquet_data_to_disk',
    python_callable=write_cbsa_parquet_data,
    dag=dag,
)

get_monitors_by_cbsa_task = PythonOperator(
    task_id='write_monitor_by_cbsa_data_to_disk',
    python_callable=get_monitors_by_cbsa,
    dag=dag
)

# create_object = S3CreateObjectOperator(
#     task_id="create_object",
#     s3_bucket=bucket_name,
#     s3_key=key,
#     data=DATA,
#     replace=True,
# )

ready = EmptyOperator(task_id='ready')

# Define task dependencies
download_cbsa_info_from_api_task >> write_cbsa_codes_to_disk_task >> ready
download_cbsa_info_from_api_task >> get_monitors_by_cbsa_task >> ready