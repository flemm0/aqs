import boto3
from airflow.models import Variable
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


aws_access_key_id = Variable.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = Variable.get('AWS_SECRET_ACCESS_KEY')


def list_objects_in_s3_bucket(**kwargs):

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    bucket_name = "airnow-aq-data-lake"
    response = s3_client.list_objects_v2(Bucket=bucket_name)

    if "Contents" in response:
        for obj in response["Contents"]:
            print(obj["Key"])
    else:
        print("No objects found in the bucket.")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'list_s3_bucket_objects',
    default_args=default_args,
    schedule_interval='@once',  # This will run the DAG once
    catchup=False  # Disable catching up for the DAG
)

list_objects_task = PythonOperator(
    task_id='list_objects_in_s3_bucket',
    python_callable=list_objects_in_s3_bucket,
    provide_context=True,
    dag=dag,
)
