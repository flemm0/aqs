from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.utils.dates import days_ago

from tasks.states import get_state_codes_from_api
from tasks.monitors import get_monitors_by_state, write_monitors_by_state_data_to_disk
from tasks.samples import get_sample_data_by_state, write_sample_data_to_disk

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

dag = DAG(
    'aqs_monitors',
    default_args=default_args,
    schedule_interval=timedelta(days=1)
)

# Define DAG tasks
get_state_codes_from_api_task = PythonOperator(
    task_id='get_state_codes_from_api_task',
    python_callable=get_state_codes_from_api,
    provide_context=True,
    dag=dag
)

date = "{{ ds_nodash }}"

get_monitors_by_state_task = PythonOperator(
    task_id='get_monitors_by_state_task',
    python_callable=get_monitors_by_state,
    provide_context=True,
    op_kwargs={'date': date},
    dag=dag
)

# create_object = S3CreateObjectOperator(
#     task_id="create_object",
#     s3_bucket='monitors',
#     s3_key=date,
#     data=get_monitors_by_state_task.output,
#     replace=True,
# )

write_monitors_by_state_data_to_disk_task = PythonOperator(
    task_id='write_monitors_by_state_data_to_disk_task',
    python_callable=write_monitors_by_state_data_to_disk,
    provide_context=True,
    op_kwargs={'date': date},
    dag=dag
)

get_sample_data_by_state_task = PythonOperator(
    task_id='get_sample_data_by_state_task',
    python_callable=get_sample_data_by_state,
    provide_context=True,
    op_kwargs={'date': date},
    dag=dag
)

write_sample_data_to_disk_task = PythonOperator(
    task_id='write_sample_data_to_disk_task',
    python_callable=write_sample_data_to_disk,
    provide_context=True,
    op_kwargs={'date': date},
    dag=dag
)


ready = EmptyOperator(task_id='ready')


# Define task dependencies
get_state_codes_from_api_task >> get_monitors_by_state_task >> write_monitors_by_state_data_to_disk_task >> ready
get_state_codes_from_api_task >> get_sample_data_by_state_task >> write_sample_data_to_disk_task >> ready