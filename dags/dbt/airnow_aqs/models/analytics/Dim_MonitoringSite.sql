with
    parameters_filtered as (
        select *
        from {{ source("staging", "stg_monitoring_sites") }}
        where parameter in ('NO2', 'PM2.5', 'PM10', 'O3')
    ),

    columns_renamed_nulls_coalesced as (
        select
            md5(concat(stationid, aqsid, fullaqsid)) as monitoring_site_key,
            stationid as station_id,
            aqsid as aqsid,
            fullaqsid as full_aqsid,
            parameter as parameter,
            monitortype as monitor_type,
            coalesce(sitecode, 9999999999) as site_code,
            sitename as site_name,
            status as status,
            agencyid as agency_id,
            agencyname as agency_name,
            coalesce(eparegion, 'Missing') as epa_region,
            latitude as latitude,
            longitude as longitude,
            coalesce(elevation, 9999999999) as elevation,
            coalesce(countryfips, 'Missing') as country_fips,
            coalesce(cbsa_id, 'Missing') as cbsa_id,
            coalesce(cbsa_name, 'Missing') as cbsa_name,
            coalesce(stateaqscode, 9999999999) as state_aqs_code,
            coalesce(stateabbreviation, 'Missing') as state_abbrevation,
            coalesce(countryaqscode, 9999999999) as country_aqs_code,
            coalesce(countryname, 'Missing') as country_name,
            validdate as valid_date
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
        select distinct
            monitoring_site_key,
            case
                when
                    monitoring_site_key
                    in (select monitoring_site_key from measures_no2)
                then 'yes'
                else 'no'
            end as measures_no2,
            case
                when
                    monitoring_site_key
                    in (select monitoring_site_key from measures_pm25)
                then 'yes'
                else 'no'
            end as measures_pm25,
            case
                when
                    monitoring_site_key
                    in (select monitoring_site_key from measures_pm10)
                then 'yes'
                else 'no'
            end as measures_pm10,
            case
                when
                    monitoring_site_key
                    in (select monitoring_site_key from measures_ozone)
                then 'yes'
                else 'no'
            end as measures_ozone
        from columns_renamed_nulls_coalesced
    ),

    final as (
        select distinct
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
        from parameters_pivoted
        left join
            columns_renamed_nulls_coalesced
            on columns_renamed_nulls_coalesced.monitoring_site_key
            = parameters_pivoted.monitoring_site_key
    )

select *
from final
