from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator

from airflow.models import Variable

EMAIL = Variable.get('EMAIL') # needs to be set in DAG file, not imported
API_KEY = Variable.get('API_KEY')

import requests

import os

# A DAG represents a workflow, a collection of tasks
with DAG(dag_id="api_testing", start_date=datetime(2024, 2, 19), schedule="0 0 * * *") as dag:
    # Tasks are represented as operators
    hello = BashOperator(task_id="hello", bash_command="echo hello")

    @task()
    def test_airflow_variable_api_credentials():
        print(EMAIL)
        print(API_KEY)

    @task()
    def test_os_variable_api_credentials():
        print(os.environ.get('EMAIL')) # None
        print(os.environ.get('API_KEY')) # None

    @task()
    def test_response():
        url = f'https://aqs.epa.gov/data/api/list/cbsas?email={EMAIL}&key={API_KEY}'
        response = requests.get(url)
        print(response.status_code)

    # Set dependencies between tasks
    hello >> test_airflow_variable_api_credentials() >> test_os_variable_api_credentials() >> test_response()