from airflow.models import Variable

from ..hooks import SnowflakeHook




################# DuckDB/MotherDuck #################

def create_md_staging_tables_if_not_existing(**kwargs):
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




################# Snowflake #################
    
def create_snowflake_tables_if_not_existing(**kwargs):
    '''Set up Snowflake staging schema'''

    create_tables_query = '''
        CREATE SCHEMA IF NOT EXISTS staging;

        CREATE TABLE IF NOT EXISTS staging.stg_hourly_data
        (
            AQSID VARCHAR(50),
            SiteName VARCHAR(50),
            Status VARCHAR(50),
            EPARegion VARCHAR(50),
            Latitude FLOAT,
            Longitude FLOAT,
            Elevation FLOAT,
            GMTOffset NUMBER(10,0),
            CountryCode VARCHAR(50),
            StateName VARCHAR(50),
            ValidDate VARCHAR(50),
            ValidTime VARCHAR(50),
            DataSource VARCHAR(50),
            ReportingArea_PipeDelimited VARCHAR(50),
            OZONE_AQI NUMBER(10, 0),
            PM10_AQI NUMBER(10, 0),
            PM25_AQI NUMBER(10, 0),
            NO2_AQI NUMBER(10, 0),
            Ozone_Measured NUMBER(10, 0),
            PM10_Measured NUMBER(10, 0),
            PM25_Measured NUMBER(10, 0),
            NO2_Measured NUMBER(10, 0),
            PM25 FLOAT,
            PM25_Unit VARCHAR(50),
            OZONE FLOAT,
            OZONE_Unit VARCHAR(50),
            NO2 FLOAT,
            NO2_Unit VARCHAR(50),
            CO FLOAT,
            CO_Unit VARCHAR(50),
            SO2 FLOAT,
            SO2_Unit VARCHAR(50),
            PM10 FLOAT,
            PM10_Unit VARCHAR(50),
            UNIQUE (AQSID, ValidDate, ValidTime)
        );


        CREATE TABLE IF NOT EXISTS staging.stg_monitoring_sites 
        (
            StationID VARCHAR(50),
            AQSID VARCHAR(50),
            FullAQSID VARCHAR(50),
            Parameter VARCHAR(50),
            MonitorType VARCHAR(50),
            SiteCode NUMBER(10, 0),
            SiteName VARCHAR(50),
            Status VARCHAR(50),
            AgencyID VARCHAR(50),
            AgencyName VARCHAR(50),
            EPARegion VARCHAR(50),
            Latitude FLOAT,
            Longitude FLOAT,
            Elevation FLOAT,
            GMTOffset NUMBER(10, 0),
            CountryFIPS VARCHAR(50),
            CBSA_ID VARCHAR(50),
            CBSA_Name VARCHAR(50),
            StateAQSCode NUMBER(10, 0),
            StateAbbreviation VARCHAR(50),
            CountryAQSCode NUMBER(10, 0),
            CountryName VARCHAR(50),
            ValidDate DATE
        );


        CREATE TABLE IF NOT EXISTS staging.stg_reporting_areas
        (
            reporting_area VARCHAR(50),
            state_code VARCHAR(50),
            country_code VARCHAR(50),
            forecasts VARCHAR(50),
            action_day_name VARCHAR(50),
            latitude FLOAT,
            longitude FLOAT,
            gmt_offset NUMBER(10, 0),
            daylight_savings VARCHAR(50),
            standard_time_zone_label VARCHAR(50),
            daylight_savings_time_zone_label VARCHAR(50),
            twc_code VARCHAR(50),
            usa_today VARCHAR(50),
            forecast_source VARCHAR(50),
            ValidDate DATE
        );


        CREATE TABLE IF NOT EXISTS staging.stg_monitoring_sites_to_reporting_areas
        (
            ReportingAreaName VARCHAR(50),
            ReportingAreaID VARCHAR(50),
            SiteID VARCHAR(50),
            SiteName VARCHAR(50),
            SiteAgencyName VARCHAR(50),
            ValidDate VARCHAR(50)
        );
    '''

    cur = SnowflakeHook().get_conn().cursor()
    cur.execute(create_tables_query)