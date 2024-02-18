import requests
import os

import polars as pl

import sys

#sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config.constants import RAW_DATA_PATH


API_KEY = os.getenv('API_KEY')
EMAIL = os.getenv('EMAIL')

def get_monitors_by_cbsa(date: str, df: pl.DataFrame):
    '''Grabs monitor information by cbsa from AQS API'''
    cbsa_codes = pl.read_parquet(source=f'{RAW_DATA_PATH}/cbsa.parquet', columns='code').to_series().to_list()
    # cbsa_codes = df.get_column('code').to_list()
    for code in cbsa_codes:
        url = f'https://aqs.epa.gov/data/api/monitors/byCBSA?email={EMAIL}&key={API_KEY}&param=42602&bdate={date}&edate={date}&cbsa={code}'
        response = requests.get(url)

        if response.status_code != 200:
            print(f'An error has occurred: status code: {response.status_code}')
            return 
        
        data = response.json()['Data']
        df = pl.DataFrame(data)
        
        df.write_parquet(f'{RAW_DATA_PATH}/monitors/{code}_{date}.parquet')