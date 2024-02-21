import requests
import polars as pl

from config.constants import DATA_PATH

from airflow.models import Variable

EMAIL = Variable.get('EMAIL')
API_KEY = Variable.get('API_KEY')



def get_state_codes_from_api(ti, **kwargs):
    '''Grabs CBSA lists from AQS API endpoint and returns Polars DataFrame'''
    url = f'https://aqs.epa.gov/data/api/list/states?email={EMAIL}&key={API_KEY}'

    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()['Data']
        ti.xcom_push(key='state_codes', value=data)
    else:
        raise ValueError(f'API request failed with status code {response.status_code}.')