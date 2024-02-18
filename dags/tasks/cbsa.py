import requests
import os

import polars as pl

from config.constants import RAW_DATA_PATH


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