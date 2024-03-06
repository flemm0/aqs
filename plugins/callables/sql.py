from airflow.models import Variable




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
    
from ..hooks import SnowflakeHook



def create_snowflake_tables_if_not_existing(**kwargs):
    '''Set up Snowflake staging schema'''

    create_tables_queries = [
        '''
        create schema if not exists airnow_aqs.staging;
        ''',
        '''
        create table if not exists airnow_aqs.staging.stg_hourly_data
        (
            AQSID varchar(50),
            SiteName varchar(50),
            Status varchar(50),
            EPARegion varchar(50),
            Latitude float,
            Longitude float,
            Elevation float,
            GMTOffset number(10,0),
            CountryCode varchar(50),
            StateName varchar(50),
            ValidDate varchar(50),
            ValidTime varchar(50),
            DataSource varchar,
            ReportingArea_PipeDelimited varchar,
            OZONE_AQI number(10, 0),
            PM10_AQI number(10, 0),
            PM25_AQI number(10, 0),
            NO2_AQI number(10, 0),
            Ozone_Measured number(10, 0),
            PM10_Measured number(10, 0),
            PM25_Measured number(10, 0),
            NO2_Measured number(10, 0),
            PM25 float,
            PM25_Unit varchar(50),
            OZONE float,
            OZONE_Unit varchar(50),
            NO2 float,
            NO2_Unit varchar(50),
            CO float,
            CO_Unit varchar(50),
            SO2 float,
            SO2_Unit varchar(50),
            PM10 float,
            PM10_Unit varchar(50),
            unique (AQSID, ValidDate, ValidTime)
        );
        ''',
        '''
        create table if not exists airnow_aqs.staging.stg_monitoring_sites 
        (
            StationID varchar(50),
            AQSID varchar(50),
            FullAQSID varchar(50),
            Parameter varchar(50),
            MonitorType varchar(50),
            SiteCode number(10, 0),
            SiteName varchar,
            Status varchar(50),
            AgencyID varchar(50),
            AgencyName varchar,
            EPARegion varchar(50),
            Latitude float,
            Longitude float,
            Elevation float,
            GMTOffset number(10, 0),
            CountryFIPS varchar(50),
            CBSA_ID varchar(50),
            CBSA_Name varchar,
            StateAQSCode number(10, 0),
            StateAbbreviation varchar(50),
            CountryAQSCode number(10, 0),
            CountryName varchar(50),
            ValidDate date
        );
        ''',
        '''
        create table if not exists airnow_aqs.staging.stg_reporting_areas
        (
            reporting_area varchar(50),
            state_code varchar(50),
            country_code varchar(50),
            forecasts varchar(50),
            action_day_name varchar(50),
            latitude float,
            longitude float,
            gmt_offset number(10, 0),
            daylight_savings varchar(50),
            standard_time_zone_label varchar(50),
            daylight_savings_time_zone_label varchar(50),
            twc_code varchar(50),
            usa_today varchar(50),
            forecast_source varchar,
            ValidDate date
        );
        ''',
        '''
        CREATE TABLE IF NOT EXISTS airnow_aqs.staging.stg_monitoring_sites_to_reporting_areas
        (
            ReportingAreaName varchar,
            ReportingAreaID varchar(50),
            SiteID varchar,
            SiteName varchar,
            SiteAgencyName varchar,
            ValidDate varchar
        );
        '''
    ]
    
    conn = SnowflakeHook().get_conn()
    cur = conn.cursor()
    for query in create_tables_queries:
        cur.execute(query)


def insert_hourly_data_to_snowflake(**kwargs):
    '''Inserts hourly observation data to Snowflake staging table'''

    pattern = f'.*{kwargs["task_instance"].xcom_pull(task_ids="hourly_data_task_group.write_daily_aqobs_data_to_s3_task", key="aqobs_s3_object_path")}'
    print(pattern)

    conn = SnowflakeHook().get_conn()

    create_temp_stage_query = """
    create temporary stage my_s3_stage
        url = 's3://airnow-aq-data-lake'
        credentials =(AWS_KEY_ID=%s AWS_SECRET_KEY=%s)
    """
    conn.cursor().execute(
        create_temp_stage_query,
        (Variable.get('AWS_ACCESS_KEY_ID'), Variable.get('AWS_SECRET_ACCESS_KEY'),)
    )

    insert_data_query = """
    copy into airnow_aqs.staging.stg_hourly_data from (
        select
            $1:AQSID::varchar(50),
            $1:SiteName::varchar(50),
            $1:Status::varchar(50),
            $1:EPARegion::varchar(50),
            $1:Latitude::float,
            $1:Longitude::float,
            $1:Elevation::float,
            $1:GMTOffset::number(10),
            $1:CountryCode::varchar(50),
            $1:StateName::varchar(50),
            $1:ValidDate::varchar(50),
            $1:ValidTime::varchar(50),
            $1:DataSource::varchar,
            $1:ReportingArea_PipeDelimited::varchar,
            $1:OZONE_AQI::number(10, 0),
            $1:PM10_AQI::number(10, 0),
            $1:PM25_AQI::number(10, 0),
            $1:NO2_AQI::number(10, 0),
            $1:Ozone_Measured::number(10, 0),
            $1:PM10_Measured::number(10, 0),
            $1:PM25_Measured::number(10, 0),
            $1:NO2_Measured::number(10, 0),
            $1:PM25::float,
            $1:PM25_Unit::varchar(50),
            $1:OZONE::float,
            $1:OZONE_Unit::varchar(50),
            $1:NO2::float,
            $1:NO2_Unit::varchar(50),
            $1:CO::float,
            $1:CO_Unit::varchar(50),
            $1:SO2::float,
            $1:SO2_Unit::varchar(50),
            $1:PM10::float,
            $1:PM10_Unit::varchar(50)
        from @my_s3_stage
    )
    file_format = ( type = 'parquet' )
    pattern = %s;
    """
    conn.cursor().execute(insert_data_query, (pattern,))


def insert_reporting_area_data_to_snowflake(date: str, **kwargs):
    '''Inserts reporting area data from S3 to Snowflake'''

    pattern = f'.*{kwargs["task_instance"].xcom_pull(task_ids="reporting_areas_task_group.write_reporting_area_locations_to_s3_task", key="reporting_areas_s3_object_path")}'

    conn = SnowflakeHook().get_conn()

    # Create external stage
    create_temp_stage_query = """
    create temporary stage my_s3_stage
        url = 's3://airnow-aq-data-lake'
        credentials =(AWS_KEY_ID=%s AWS_SECRET_KEY=%s)
    """
    conn.cursor().execute(
        create_temp_stage_query,
        (Variable.get('AWS_ACCESS_KEY_ID'), Variable.get('AWS_SECRET_ACCESS_KEY'),)
    )

    # Create temp table
    create_temp_table_query = """
    create or replace temporary table airnow_aqs.staging.tmp_stg_reporting_areas (
        reporting_area varchar(50),
        state_code varchar(50),
        country_code varchar(50),
        forecasts varchar(50),
        action_day_name varchar(50),
        latitude float,
        longitude float,
        gmt_offset number(10, 0),
        daylight_savings varchar(50),
        standard_time_zone_label varchar(50),
        daylight_savings_time_zone_label varchar(50),
        twc_code varchar(50),
        usa_today varchar(50),
        forecast_source varchar
    );
    """
    conn.cursor().execute(create_temp_table_query)

    # Load data from stage into temp table 
    load_temp_table_query = """
    copy into airnow_aqs.staging.tmp_stg_reporting_areas from (
        select
            $1:reporting_area::varchar(50),
            $1:state_code::varchar(50),
            $1:country_code::varchar(50),
            $1:forecasts::varchar(50),
            $1:action_day_name::varchar(50),
            $1:latitude::float,
            $1:longitude::float,
            $1:gmt_offset::number(10, 0),
            $1:daylight_savings::varchar(50),
            $1:standard_time_zone_label::varchar(50),
            $1:daylight_savings_time_zone_label::varchar(50),
            $1:twc_code::varchar(50),
            $1:usa_today::varchar(50),
            $1:forecast_source::varchar
        from @my_s3_stage
    )
    file_format = ( type = 'parquet' )
    pattern = %s;
    """
    conn.cursor().execute(
        load_temp_table_query,
        (pattern,)
    )

    # Insert new rows
    insert_new_rows_query = """
    insert into airnow_aqs.staging.stg_reporting_areas
    select *, %s as ValidDate
    from
    (
        select *
        from airnow_aqs.staging.tmp_stg_reporting_areas
        except
        select * exclude(ValidDate)
        from airnow_aqs.staging.stg_reporting_areas
    );
    """
    conn.cursor().execute(
        insert_new_rows_query,
        (date,)
    )

    # Remove temp table
    drop_temp_table_query = "drop table if exists airnow_aqs.staging.tmp_stg_reporting_areas;"
    conn.cursor().execute(drop_temp_table_query)


def insert_monitoring_site_data_to_snowflake(date, **kwargs) -> None:
    '''Inserts monitoring site data from S3 into Snowflake'''

    pattern = f'.*{kwargs["task_instance"].xcom_pull(task_ids="monitoring_sites_task_group.write_monitoring_site_locations_to_s3_task", key="monitoring_sites_s3_object_path")}'

    conn = SnowflakeHook().get_conn()

    # Create external stage
    create_temp_stage_query = """
    create temporary stage my_s3_stage
        url = 's3://airnow-aq-data-lake'
        credentials =(AWS_KEY_ID=%s AWS_SECRET_KEY=%s)
    """
    conn.cursor().execute(
        create_temp_stage_query,
        (Variable.get('AWS_ACCESS_KEY_ID'), Variable.get('AWS_SECRET_ACCESS_KEY'),)
    )

    # Create temp table
    create_temp_table_query = """
    create or replace temporary table airnow_aqs.staging.tmp_stg_monitoring_sites (
        StationID varchar(50),
        AQSID varchar(50),
        FullAQSID varchar(50),
        Parameter varchar(50),
        MonitorType varchar(50),
        SiteCode number(10, 0),
        SiteName varchar,
        Status varchar(50),
        AgencyID varchar(50),
        AgencyName varchar,
        EPARegion varchar(50),
        Latitude float,
        Longitude float,
        Elevation float,
        GMTOffset number(10, 0),
        CountryFIPS varchar(50),
        CBSA_ID varchar(50),
        CBSA_Name varchar,
        StateAQSCode number(10, 0),
        StateAbbreviation varchar(50),
        CountryAQSCode number(10, 0),
        CountryName varchar(50)
    );
    """
    conn.cursor().execute(create_temp_table_query)

    # Load data from stage into temp table 
    load_temp_table_query = """
    copy into airnow_aqs.staging.tmp_stg_monitoring_sites from (
        select
            $1:StationID::varchar(50),
            $1:AQSID::varchar(50),
            $1:FullAQSID::varchar(50),
            $1:Parameter::varchar(50),
            $1:MonitorType::varchar(50),
            $1:SiteCode::number(10, 0),
            $1:SiteName::varchar,
            $1:Status::varchar(50),
            $1:AgencyID::varchar(50),
            $1:AgencyName::varchar,
            $1:EPARegion::varchar(50),
            $1:Latitude::float,
            $1:Longitude::float,
            $1:Elevation::float,
            $1:GMTOffset::number(10, 0),
            $1:CountryFIPS::varchar(50),
            $1:CBSA_ID::varchar(50),
            $1:CBSA_Name::varchar,
            $1:StateAQSCode::number(10, 0),
            $1:StateAbbreviation::varchar(50),
            $1:CountryAQSCode::number(10, 0),
            $1:CountryName::varchar(50)
        from @my_s3_stage
    )
    file_format = ( type = 'parquet' )
    pattern = %s;
    """
    conn.cursor().execute(
        load_temp_table_query,
        (pattern,)
    )

    # Insert new rows
    insert_new_rows_query = """
    insert into airnow_aqs.staging.stg_monitoring_sites
    select *, %s as ValidDate
    from
    (
        select *
        from airnow_aqs.staging.tmp_stg_monitoring_sites
        except
        select * exclude(ValidDate)
        from airnow_aqs.staging.stg_monitoring_sites
    );
    """
    conn.cursor().execute(
        insert_new_rows_query,
        (date,)
    )

    # Remove temp table
    drop_temp_table_query = "drop table if exists airnow_aqs.staging.tmp_stg_monitoring_sites;"
    conn.cursor().execute(drop_temp_table_query)


def load_monitoring_sites_to_reporting_areas_to_snowflake(date: str, **kwargs) -> None:
    '''Inserts monitoring site to reporting area mapping from S3 into Snowflake'''

    pattern = f'.*{kwargs["task_instance"].xcom_pull(task_ids="monitoring_sites_to_reporting_areas_task_group.write_monitoring_sites_to_reporting_areas_to_s3_task", key="monitoring_sites_to_reporting_areas_s3_object_path")}'

    conn = SnowflakeHook().get_conn()

    # Create external stage
    create_temp_stage_query = """
    create temporary stage my_s3_stage
        url = 's3://airnow-aq-data-lake'
        credentials =(AWS_KEY_ID=%s AWS_SECRET_KEY=%s)
    """
    conn.cursor().execute(
        create_temp_stage_query,
        (Variable.get('AWS_ACCESS_KEY_ID'), Variable.get('AWS_SECRET_ACCESS_KEY'),)
    )

    # Create temp table
    create_temp_table_query = """
    create or replace temporary table airnow_aqs.staging.tmp_stg_monitoring_sites_to_reporting_areas (
        ReportingAreaName varchar,
        ReportingAreaID varchar(50),
        SiteID varchar,
        SiteName varchar,
        SiteAgencyName varchar
    );
    """
    conn.cursor().execute(create_temp_table_query)

    # Load data from stage into temp table 
    load_temp_table_query = """
    copy into airnow_aqs.staging.tmp_stg_monitoring_sites_to_reporting_areas from (
        select
            $1:ReportingAreaName::varchar,
            $1:ReportingAreaID::varchar(50),
            $1:SiteID::varchar,
            $1:SiteName::varchar,
            $1:SiteAgencyName::varchar
        from @my_s3_stage
    )
    file_format = ( type = 'parquet' )
    pattern = %s;
    """
    conn.cursor().execute(
        load_temp_table_query,
        (pattern,)
    )

    # Insert new rows
    insert_new_rows_query = """
    insert into airnow_aqs.staging.stg_monitoring_sites_to_reporting_areas
    select *, %s as ValidDate
    from
    (
        select *
        from airnow_aqs.staging.tmp_stg_monitoring_sites_to_reporting_areas
        except
        select * exclude(ValidDate)
        from airnow_aqs.staging.stg_monitoring_sites_to_reporting_areas
    );
    """
    conn.cursor().execute(
        insert_new_rows_query,
        (date,)
    )
    
    # Remove temp table
    drop_temp_table_query = "drop table if exists airnow_aqs.staging.tmp_stg_monitoring_sites_to_reporting_areas;"
    conn.cursor().execute(drop_temp_table_query)