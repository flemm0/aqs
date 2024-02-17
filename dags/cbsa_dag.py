from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator

import polars as pl
import requests
import os

from constants import RAW_DATA_PATH

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

## TODO: move functions to plugins folder
def get_cbsa_codes() -> pl.DataFrame:
    '''Grabs CBSA lists from AQS API endpoint and returns Polars DataFrame'''

    API_KEY = os.getenv('API_KEY')
    EMAIL = os.getenv('EMAIL')

    url = f'https://aqs.epa.gov/data/api/list/cbsas?email={EMAIL}&key={API_KEY}'

    response = requests.get(url)

    if response.status_code != 200:
        print(f'An error has occurred: status code: {response.status_code}')
        return 

    data = response.json()['Data']
    df = pl.DataFrame(data)

    return df

def write_cbsa_parquet_data(df: pl.DataFrame) -> None:
    '''Accepts cbsa codes in Polars DataFrame and writes out parquet file to data lake'''
    df.write_parquet(f'{RAW_DATA_PATH}/cbsa.parquet')

# Define DAG tasks
download_from_api_task = PythonOperator(
    task_id='fetch_raw_cbsa_codes_from_api',
    python_callable=get_cbsa_codes,
    dag=dag,
)

write_parquet_to_disk_task = PythonOperator(
    task_id='write_cbsa_codes_parquet_data_to_disk',
    python_callable=write_cbsa_parquet_data,
    dag=dag,
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
download_from_api_task >> write_parquet_to_disk_task >> ready