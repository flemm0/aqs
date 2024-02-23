from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

from airflow.models import Variable

EMAIL = Variable.get('EMAIL') # needs to be set in DAG file, not imported
API_KEY = Variable.get('API_KEY')

import requests

import os

# A DAG represents a workflow, a collection of tasks
with DAG(dag_id="testing_dag", start_date=days_ago(4), end_date=days_ago(4, hour=23), schedule="0 0 * * *") as dag:

    # Tasks are represented as operators
    hello = BashOperator(task_id="hello", bash_command="echo hello")

    # @task()
    # def test_airflow_variable_api_credentials():
    #     print(EMAIL)
    #     print(API_KEY)

    # @task()
    # def test_response():
    #     url = f'https://aqs.epa.gov/data/api/list/cbsas?email={EMAIL}&key={API_KEY}'
    #     response = requests.get(url)
    #     print(response.status_code)

    echo_dates = BashOperator(
        task_id='echo_ds',
        bash_command='''
        echo todays date is: "{{ ds }}"
        echo data_interval_start is: "{{ data_interval_start }}"
        echo data_interval_end is: "{{ data_interval_end }}"
        echo yesterdays date is: "{{ macros.ds_add(ds, -1) }}"
        echo yesterdays formatted date is: "{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%Y%m%d') }}"
        '''
    )

    def print_url_to_get(date: str):
        url = f'https://s3-us-west-1.amazonaws.com/files.airnowtech.org/airnow/{date[:4]}/{date}/HourlyAQObs_{date}00.dat'
        print(url)

    print_url_task = PythonOperator(
        task_id='print_url',
        python_callable=print_url_to_get,
        op_kwargs={'date': "{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%Y%m%d') }}" },
        provide_context=True
    )
    
    done = EmptyOperator(task_id='done')

    # Set dependencies between tasks
    hello >> echo_dates >> print_url_task >> done