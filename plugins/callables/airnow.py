from airflow.models import Variable

import requests
from io import BytesIO
import os

import boto3
from botocore.exceptions import NoCredentialsError

from config.schemas import hourly_aqobs_file_schema, reporting_area_locations_v2_schema, monitoring_site_location_v2_schema
from config.constants import DATA_PATH



################# HourlyAQObs Data Callables #################

def extract_aqobs_daily_data(date: str, **kwargs):
    '''Extracts and dumps HourlyAQObs data files'''
    import polars as pl

    data = []
    for hour in ["{:02d}".format(i) for i in range(0, 24)]:
        url = f'https://s3-us-west-1.amazonaws.com/files.airnowtech.org/airnow/{date[:4]}/{date}/HourlyAQObs_{date}{hour}.dat'
        print(f'Extracting file: HourlyAQObs_{date}{hour}.dat')
        response = requests.get(url)
        df = pl.read_csv(BytesIO(response.content), ignore_errors=True, schema=hourly_aqobs_file_schema)
        data.extend(df.to_dicts())
    
    df = pl.DataFrame(data, schema=hourly_aqobs_file_schema)

    path = DATA_PATH / 'hourly_data' / date[:4]
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)
    
    file_path = f'{path}/{date}.parquet'
    df.write_parquet(file=file_path)
    
    kwargs['ti'].xcom_push(key='aqobs_data', value=file_path)


def write_daily_aqobs_data_to_s3(**kwargs):
    '''Writes HourlyAQObs data files to s3 bucket'''
    aws_access_key_id = Variable.get('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = Variable.get('AWS_SECRET_ACCESS_KEY')

    file_path = kwargs['ti'].xcom_pull(task_ids='hourly_data_task_group.extract_aqobs_daily_data_task', key='aqobs_data')

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

    kwargs['ti'].xcom_push(key='aqobs_s3_object_path', value='/'.join(file_path.split('/')[1:]))



################# Reporting Area Locations Callables #################

def extract_reporting_area_locations(date: str, **kwargs):
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

    kwargs['ti'].xcom_push(key='reporting_area_locations', value=f'{path}/{date}.parquet')

def write_reporting_area_locations_to_s3(**kwargs):
    '''Writes reporting area locations data files to s3 bucket'''
    aws_access_key_id = Variable.get('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = Variable.get('AWS_SECRET_ACCESS_KEY')

    file_path = kwargs['ti'].xcom_pull(task_ids='reporting_areas_task_group.extract_reporting_area_locations_task', key='reporting_area_locations')

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

    kwargs['ti'].xcom_push(key='reporting_areas_s3_object_path', value='/'.join(file_path.split('/')[1:]))



################# Monitoring Site Locations Callables #################
    
def extract_monitoring_site_locations(date: str, **kwargs):
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

    kwargs['ti'].xcom_push(key='monitoring_site_locations', value=f'{path}/{date}.parquet')

def write_monitoring_site_locations_to_s3(**kwargs):
    '''Writes monitoring site locations data files to s3 bucket'''
    aws_access_key_id = Variable.get('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = Variable.get('AWS_SECRET_ACCESS_KEY')

    file_path = kwargs['ti'].xcom_pull(task_ids='monitoring_sites_task_group.extract_monitoring_site_locations_task', key='monitoring_site_locations')

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

    kwargs['ti'].xcom_push(key='monitoring_sites_s3_object_path', value='/'.join(file_path.split('/')[1:]))



################# Monitoring Sites to Reporting Areas Callables #################

def extract_monitoring_sites_to_reporting_areas(date: str, **kwargs):
    '''Extracts data files mapping monitoring sites to reporting areas'''
    import polars as pl

    url = f'https://s3-us-west-1.amazonaws.com//files.airnowtech.org/airnow/{date[:4]}/{date}/Site_To_ReportingArea.csv'
    print(f'Extracting file: Site_To_ReportingArea.csv')
    response = requests.get(url)

    df = pl.read_csv(
        BytesIO(response.content),
        ignore_errors=True,
    )

    df = df.select(['ReportingAreaName', 'ReportingAreaID', 'SiteID', 'SiteName', 'SiteAgencyName'])

    path = DATA_PATH / 'monitoring_sites_to_reporting_areas' / date[:4]
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)

    df.write_parquet(f'{path}/{date}.parquet')

    kwargs['ti'].xcom_push(key='monitoring_sites_to_reporting_areas', value=f'{path}/{date}.parquet')

def write_monitoring_sites_to_reporting_areas_to_s3(**kwargs):
    '''Writes monitoring site to reporting areas mapping to s3 bucket'''
    aws_access_key_id = Variable.get('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = Variable.get('AWS_SECRET_ACCESS_KEY')

    file_path = kwargs['ti'].xcom_pull(
        task_ids='monitoring_sites_to_reporting_areas_task_group.extract_monitoring_sites_to_reporting_areas_task', 
        key='monitoring_sites_to_reporting_areas'
    )

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

    kwargs['ti'].xcom_push(key='monitoring_sites_to_reporting_areas_s3_object_path', value='/'.join(file_path.split('/')[1:]))



################# SQL #################

def create_staging_tables_if_not_existing(**kwargs):
    '''Set up staging tables'''
    import duckdb

    create_staging_tables_query = '''
        CREATE SCHEMA IF NOT EXISTS staging;

        CREATE TABLE IF NOT EXISTS staging.stg_hourly_data
        (
            AQSID VARCHAR,
            SiteName VARCHAR,
            Status VARCHAR,
            EPARegion VARCHAR,
            Latitude DOUBLE,
            Longitude DOUBLE,
            Elevation DOUBLE,
            GMTOffset INTEGER,
            CountryCode VARCHAR,
            StateName VARCHAR,
            ValidDate VARCHAR,
            ValidTime VARCHAR,
            DataSource VARCHAR,
            ReportingArea_PipeDelimited VARCHAR,
            OZONE_AQI INTEGER,
            PM10_AQI INTEGER,
            PM25_AQI INTEGER,
            NO2_AQI INTEGER,
            Ozone_Measured INTEGER,
            PM10_Measured INTEGER,
            PM25_Measured INTEGER,
            NO2_Measured INTEGER,
            PM25 DOUBLE,
            PM25_Unit VARCHAR,
            OZONE DOUBLE,
            OZONE_Unit VARCHAR,
            NO2 DOUBLE,
            NO2_Unit VARCHAR,
            CO DOUBLE,
            CO_Unit VARCHAR,
            SO2 DOUBLE,
            SO2_Unit VARCHAR,
            PM10 DOUBLE,
            PM10_Unit VARCHAR,
            UNIQUE (AQSID, ValidDate, ValidTime)
        );

        CREATE TABLE IF NOT EXISTS staging.stg_monitoring_sites 
        (
            StationID VARCHAR,
            AQSID VARCHAR,
            FullAQSID VARCHAR,
            Parameter VARCHAR,
            MonitorType VARCHAR,
            SiteCode INTEGER,
            SiteName VARCHAR,
            Status VARCHAR,
            AgencyID VARCHAR,
            AgencyName VARCHAR,
            EPARegion VARCHAR,
            Latitude DOUBLE,
            Longitude DOUBLE,
            Elevation DOUBLE,
            GMTOffset INTEGER,
            CountryFIPS VARCHAR,
            CBSA_ID VARCHAR,
            CBSA_Name VARCHAR,
            StateAQSCode INTEGER,
            StateAbbreviation VARCHAR,
            CountryAQSCode INTEGER,
            CountryName VARCHAR,
            ValidDate DATE
        );

        CREATE TABLE IF NOT EXISTS staging.stg_reporting_areas
        (
            reporting_area VARCHAR,
            state_code VARCHAR,
            country_code VARCHAR,
            forecasts VARCHAR,
            action_day_name VARCHAR,
            latitude DOUBLE,
            longitude DOUBLE,
            gmt_offset INTEGER,
            daylight_savings VARCHAR,
            standard_time_zone_label VARCHAR,
            daylight_savings_time_zone_label VARCHAR,
            twc_code VARCHAR,
            usa_today VARCHAR,
            forecast_source VARCHAR,
            ValidDate DATE
        );

        CREATE TABLE IF NOT EXISTS staging.stg_monitoring_sites_to_reporting_areas
        (
            ReportingAreaName VARCHAR,
            ReportingAreaID VARCHAR,
            SiteID VARCHAR,
            SiteName VARCHAR,
            SiteAgencyName VARCHAR,
            ValidDate VARCHAR
        )
    '''

    motherduck_token = Variable.get('MOTHERDUCK_TOKEN')
    conn = duckdb.connect(f'md:airnow_aqs?motherduck_token={motherduck_token}')
    conn.execute(create_staging_tables_query)

def drop_temp_table(table: str, **kwargs):
    import duckdb
    motherduck_token = Variable.get('MOTHERDUCK_TOKEN')
    '''Drops temp table created in staging table updates'''
    drop_table_query = f'''
        DROP TABLE IF EXISTS {table}
    '''
    conn = duckdb.connect(f'md:airnow_aqs?motherduck_token={motherduck_token}')
    conn.execute(drop_table_query)