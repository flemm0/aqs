import requests
import polars as pl

from config.constants import DATA_PATH
from config.schemas import monitor_json_schema

from airflow.models import Variable

EMAIL = Variable.get('EMAIL')
API_KEY = Variable.get('API_KEY')


def get_monitors_by_cbsa(ti, date: str, **kwargs):
    '''Grabs monitor information by cbsa from AQS API for specified date'''
    data = ti.xcom_pull(task_ids='download_cbsa_info_from_api_task', key='cbsa_data')
    df = pl.DataFrame(data)
    cbsa_codes = df.get_column('code').to_list()

    # Make monitor "bucket"
    path = DATA_PATH / 'monitors'
    if not path.exists():
        path.mkdir(exist_ok=True)

    checked = 0
    for code in cbsa_codes:
        url = f'https://aqs.epa.gov/data/api/monitors/byCBSA?email={EMAIL}&key={API_KEY}&param=42602&bdate={date}&edate={date}&cbsa={code}'
        response = requests.get(url)

        if response.status_code != 200:
            print(f'An error has occurred: status code: {response.status_code}')
        else: 
            data = response.json()['Data']
            if data:
                df = pl.DataFrame(data, schema=monitor_json_schema)          
                df.write_parquet(f'{DATA_PATH}/monitors/{code}_{date}.parquet')
            else:
                print(f'No data for code: {code} on date: {date}')
        # logging info
        checked += 1
        print(f'Percent CBSAs checked: {checked // len(cbsa_codes)}%')