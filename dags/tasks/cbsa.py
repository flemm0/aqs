import requests
import polars as pl

from config.constants import DATA_PATH

from airflow.models import Variable

EMAIL = Variable.get('EMAIL')
API_KEY = Variable.get('API_KEY')

def get_cbsa_codes(ti, **kwargs):
    '''Grabs CBSA lists from AQS API endpoint and returns Polars DataFrame'''
    url = f'https://aqs.epa.gov/data/api/list/cbsas?email={EMAIL}&key={API_KEY}'

    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()['Data']
        ti.xcom_push(key='cbsa_data', value=data)
    else:
        raise ValueError(f'API request failed with status code {response.status_code}.')

def write_cbsa_parquet_data(ti, **kwargs):
    '''Accepts cbsa codes in json data and writes out parquet file to data lake'''
    data = ti.xcom_pull(task_ids='download_cbsa_info_from_api_task', key='cbsa_data')
    if not data:
        raise Exception('No data pulled by x_com')
    df = pl.DataFrame(data)
    path = DATA_PATH / 'cbsa.parquet'
    df.write_parquet(path)