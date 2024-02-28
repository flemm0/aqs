from airflow.models import Variable

import requests
from io import BytesIO
import os

import boto3
from botocore.exceptions import NoCredentialsError

from config.schemas import hourly_aqobs_file_schema, reporting_area_locations_v2_schema, monitoring_site_location_v2_schema
from config.constants import DATA_PATH


# HourlyAQObs Data Callables

def extract_aqobs_daily_data(ti, date: str, **kwargs):
    '''Extracts and dumps HourlyAQObs data files'''
    import polars as pl

    data = []
    for hour in ["{:02d}".format(i) for i in range(0, 24)]:
        url = f'https://s3-us-west-1.amazonaws.com/files.airnowtech.org/airnow/{date[:4]}/{date}/HourlyAQObs_{date}{hour}.dat'
        print(f'Extracting file: HourlyAQObs_{date}{hour}.dat')
        response = requests.get(url)
        df = pl.read_csv(BytesIO(response.content), ignore_errors=True, schema=hourly_aqobs_file_schema)
        data.extend(df.to_dicts())
    
    df = pl.DataFrame(data)

    path = DATA_PATH / 'hourly_data' / date[:4]
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)
    
    df.write_parquet(f'{path}/{date}.parquet')
    
    ti.xcom_push(key='aqobs_data', value=f'{path}/{date}.parquet')

def write_daily_aqobs_data_to_s3(ti, **kwargs):
    '''Writes HourlyAQObs data files to s3 bucket'''
    aws_access_key_id = Variable.get('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = Variable.get('AWS_SECRET_ACCESS_KEY')

    file_path = ti.xcom_pull(task_ids='extract_aqobs_daily_data_task', key='aqobs_data')

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    bucket_name = "airnow-aq-data-lake"

    print(f"Writing S3 file: {bucket_name}/{'/'.join(file_path.split('/')[1:])}")

    try:
        s3_client.upload_file(file_path, bucket_name, '/'.join(file_path.split('/')[1:]))
        print("Upload Successful")
    except FileNotFoundError:
        print("The file was not found")
    except NoCredentialsError:
        print("Credentials not available")

    ti.xcom_push(key='aqobs_s3_object_path', value='/'.join(file_path.split('/')[1:]))

def cleanup_local_hourly_data_files(ti, **kwargs):
    '''Cleanup files at data/hourly_data/ '''
    file_path = ti.xcom_pull(task_ids='extract_aqobs_daily_data_task', key='aqobs_data')
    os.remove(file_path)


# Reporting Area Locations Callables

def extract_reporting_area_locations(ti, date: str, **kwargs):
    '''Extracts data files provided on the reporting areas'''
    import polars as pl

    url = f'https://s3-us-west-1.amazonaws.com/files.airnowtech.org/airnow/{date[:4]}/{date}/reporting_area_locations_V2.dat'
    print(f'Extracting file: reporting_area_locations_V2.dat')
    response = requests.get(url)
    
    df = pl.read_csv(
        BytesIO(response.content),
        ignore_errors=True,
        separator='|',
        schema=reporting_area_locations_v2_schema
    )

    path = DATA_PATH / 'reporting_areas' / date[:4]
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)

    df.write_parquet(f'{path}/{date}.parquet')

    ti.xcom_push(key='reporting_area_locations', value=f'{path}/{date}.parquet')

def write_reporting_area_locations_to_s3(ti, **kwargs):
    '''Writes reporting area locations data files to s3 bucket'''
    aws_access_key_id = Variable.get('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = Variable.get('AWS_SECRET_ACCESS_KEY')

    file_path = ti.xcom_pull(task_ids='extract_reporting_area_locations_task', key='reporting_area_locations')

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    bucket_name = "airnow-aq-data-lake"

    print(f"Writing S3 file: {bucket_name}/{'/'.join(file_path.split('/')[1:])}")

    try:
        s3_client.upload_file(file_path, bucket_name, '/'.join(file_path.split('/')[1:]))
        print("Upload Successful")
    except FileNotFoundError:
        print("The file was not found")
    except NoCredentialsError:
        print("Credentials not available")

    ti.xcom_push(key='reporting_areas_s3_object_path', value='/'.join(file_path.split('/')[1:]))

def cleanup_reporting_area_location_files(ti, **kwargs):
    '''Cleanup files at data/reporting_areas/ '''
    file_path = ti.xcom_pull(task_ids='extract_reporting_area_locations_task', key='reporting_area_locations')
    os.remove(file_path)


# Monitoring Site Locations Callables
    
def extract_monitoring_site_locations(ti, date: str, **kwargs):
    '''Extracts data files provided on monitorinig sites'''
    import polars as pl

    url = f'https://s3-us-west-1.amazonaws.com//files.airnowtech.org/airnow/{date[:4]}/{date}/Monitoring_Site_Locations_V2.dat'
    print(f'Extracting file: Monitoring_Site_Locations_V2.dat')
    response = requests.get(url)

    df = pl.read_csv(
        BytesIO(response.content),
        ignore_errors=True,
        separator='|',
        schema=monitoring_site_location_v2_schema
    )

    path = DATA_PATH / 'monitoring_sites' / date[:4]
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)

    df.write_parquet(f'{path}/{date}.parquet')

    ti.xcom_push(key='monitoring_site_locations', value=f'{path}/{date}.parquet')

def write_monitoring_site_locations_to_s3(ti, **kwargs):
    '''Writes monitoring site locations data files to s3 bucket'''
    aws_access_key_id = Variable.get('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = Variable.get('AWS_SECRET_ACCESS_KEY')

    file_path = ti.xcom_pull(task_ids='extract_monitoring_site_locations_task', key='monitoring_site_locations')

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    bucket_name = "airnow-aq-data-lake"

    print(f"Writing S3 file: {bucket_name}/{'/'.join(file_path.split('/')[1:])}")

    try:
        s3_client.upload_file(file_path, bucket_name, '/'.join(file_path.split('/')[1:]))
        print("Upload Successful")
    except FileNotFoundError:
        print("The file was not found")
    except NoCredentialsError:
        print("Credentials not available")

    ti.xcom_push(key='monitoring_sites_s3_object_path', value='/'.join(file_path.split('/')[1:]))

def cleanup_monitoring_site_location_files(ti, **kwargs):
    '''Cleanup files at data/moinitoring_sites/ '''
    file_path = ti.xcom_pull(task_ids='extract_monitoring_site_locations_task', key='monitoring_site_locations')
    os.remove(file_path)