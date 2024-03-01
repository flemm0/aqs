with parameters_filtered as (
    select *
    from {{ source('staging', 'stg_monitoring_sites') }}
    where Parameter in ('NO2', 'PM2.5', 'PM10', 'O3')
),

columns_renamed_nulls_coalesced as (
    select
        md5( concat(StationID, AQSID, FullAQSID) ) as monitoring_site_key,
        StationID as station_id,
        AQSID as aqsid,
        FullAQSID as full_aqsid,
        Parameter as parameter,
        MonitorType as monitor_type,
        coalesce( SiteCode, 9999999999 ) as site_code,
        SiteName as site_name,
        Status as status,
        AgencyID as agency_id,
        AgencyName as agency_name,
        coalesce ( EPARegion, 'Missing' ) as epa_region,
        Latitude as latitude,
        Longitude as longitude,
        coalesce( Elevation, 9999999999 ) as elevation,
        coalesce( CountryFIPS, 'Missing' ) as country_fips,
        coalesce( CBSA_ID, 'Missing' ) as cbsa_id,
        coalesce( CBSA_Name, 'Missing' ) as cbsa_name,
        coalesce( StateAQSCode, 9999999999 ) as state_aqs_code,
        coalesce( StateAbbreviation, 'Missing' ) as state_abbrevation,
        coalesce( CountryAQSCode, 9999999999 ) as country_aqs_code,
        coalesce( CountryName, 'Missing' ) as country_name,
        ValidDate as valid_date
    from parameters_filtered
),


measures_no2 as (
    select monitoring_site_key
    from columns_renamed_nulls_coalesced
    where parameter = 'NO2'
),

measures_pm25 as (
    select monitoring_site_key
    from columns_renamed_nulls_coalesced
    where parameter = 'PM2.5'
),

measures_pm10 as (
    select monitoring_site_key
    from columns_renamed_nulls_coalesced
    where parameter = 'PM10'
),

measures_ozone as (
    select monitoring_site_key
    from columns_renamed_nulls_coalesced
    where parameter = 'O3'
),

parameters_pivoted as (
    select 
        distinct
        monitoring_site_key,
        case
            when monitoring_site_key in ( select monitoring_site_key from measures_no2 )
            then 'yes'
            else 'no'
        end as measures_no2,
        case
            when monitoring_site_key in ( select monitoring_site_key from measures_pm25 )
            then 'yes'
            else 'no'
        end as measures_pm25,
        case 
            when monitoring_site_key in ( select monitoring_site_key from measures_pm10 )
            then 'yes'
            else 'no'
        end as measures_pm10,
        case
            when monitoring_site_key in ( select monitoring_site_key from measures_ozone )
            then 'yes'
            else 'no'
        end as measures_ozone
    from columns_renamed_nulls_coalesced
),

final as (
    select 
        distinct
        parameters_pivoted.monitoring_site_key,
        station_id,
        full_aqsid,
        monitor_type,
        site_code,
        site_name,
        status,
        agency_id,
        agency_name,
        epa_region,
        latitude,
        longitude,
        elevation,
        country_fips,
        cbsa_id,
        cbsa_name,
        state_aqs_code,
        state_abbrevation,
        country_aqs_code,
        measures_no2,
        measures_pm25,
        measures_pm10,
        measures_ozone,
        valid_date
    from
        parameters_pivoted
    left join
        columns_renamed_nulls_coalesced
    on columns_renamed_nulls_coalesced.monitoring_site_key = parameters_pivoted.monitoring_site_key
)

select * from final