{{
    config(
        materialized='incremental'
    )
}}

with 
    new_rows as (
        select *
        from {{ source('staging', 'stg_hourly_data') }}

        {% if is_incremental() %}

        where strptime(ValidDate, '%m/%d/%Y') > ( select max(strptime(date_key, '%Y%m%d')) from {{ this }})

        {% endif %}
),

    monitoring_site_surrogate_keys_joined as (
        select 
            monitoring_site_key,
            new_rows.*
        from new_rows
        join {{ ref('Dim_MonitoringSite') }}
        on {{ ref('Dim_MonitoringSite') }}.site_name = new_rows.SiteName
),
    
    reporting_area_surrogate_keys_joined as (
        select
            reporting_area_key,
            monitoring_site_surrogate_keys_joined.*
        from monitoring_site_surrogate_keys_joined
        join {{ ref('Bridge_MonitoringSite_ReportingArea') }}
        on monitoring_site_surrogate_keys_joined.monitoring_site_key = {{ ref('Bridge_MonitoringSite_ReportingArea') }}.monitoring_site_key
),

    columns_filtered as (
        select
            monitoring_site_key,
            reporting_area_key,
            strftime(strptime(ValidDate, '%m/%d/%Y'), '%Y%m%d') as date_key,
            cast( ValidTime[:2] as integer ) as hour,
            OZONE_AQI as ozone_aqi,
            OZONE as ozone_concentration,
            OZONE_Unit as ozone_unit,
            PM10_AQI as pm10_aqi,
            PM10 as pm10_concentration,
            PM10_Unit as pm10_unit,
            PM25_AQI as pm25_aqi,
            PM25 as pm25_concentration,
            PM25_Unit as pm25_unit,
            NO2_AQI as no2_aqi,
            NO2 as no2_concentration,
            NO2_Unit as no2_unit
        from reporting_area_surrogate_keys_joined
        order by date_key, hour
)

select *
from columns_filtered
