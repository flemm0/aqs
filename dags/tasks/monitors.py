import requests
import polars as pl

from config.constants import DATA_PATH
from config.schemas import monitor_json_schema

from airflow.models import Variable

EMAIL = Variable.get('EMAIL')
API_KEY = Variable.get('API_KEY')



def get_monitors_by_state(ti, date: str, **kwargs):
    '''Grabs monitor information by cbsa from AQS API for specified date'''
    data = ti.xcom_pull(task_ids='get_state_codes_from_api_task', key='state_codes')
    state_codes = [val['code'] for val in data]

    checked, data_list = 0, []
    for code in state_codes:
        url = f'https://aqs.epa.gov/data/api/monitors/byState?email={EMAIL}&key={API_KEY}&param=42602&bdate={date}&edate={date}&state={code}'
        response = requests.get(url)

        if response.status_code != 200:
            print(f'An error has occurred: status code: {response.status_code}')
        else: 
            data = response.json()['Data']
            if data:
                data_list.extend(data)
            else:
                print(f'No data for state code: {code} on date: {date}')

        # logging info
        checked += 1
        print(f'{checked} states checked out of {len(state_codes)}')

    ti.xcom_push(key='monitor_data', value=data_list)


def write_monitors_by_state_data_to_disk(ti, date: str, **kwargs):
    '''Writes monitor information by cbsa to disk'''

    # create monitors "bucket"
    path = DATA_PATH / 'monitors'
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)

    data = ti.xcom_pull(task_ids='get_monitors_by_state_task', key='monitor_data')
    df = pl.DataFrame(data, schema=monitor_json_schema)
    df.write_parquet(f'{path}/{date}.parquet')