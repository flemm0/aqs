with
    columns_filtered as (
        select
            date_key,
            reporting_area_key,
            pm10_aqi,
            pm25_aqi,
            ozone_aqi,
            no2_aqi
        from {{ ref("Fact_AQObs") }}
    ),

    joined as (
        select
            full_date,
            latitude,
            longitude,
            reporting_area,
            pm10_aqi,
            pm25_aqi,
            ozone_aqi,
            no2_aqi
        from columns_filtered
        join {{ ref("Dim_ReportingArea") }}
        on {{ ref("Dim_ReportingArea") }}.reporting_area_key 
        = columns_filtered.reporting_area_key
        join {{ ref("Dim_Date" )}}
        on {{ ref("Dim_Date" )}}.date_key
        = columns_filtered.date_key
    ),

    categories_added as (
        select
            *,
            {{ assign_aqi_category(pm10_aqi) }} as pm10_aqi_category,
            {{ assign_aqi_category(pm25_aqi) }} as pm25_aqi_category,
            {{ assign_aqi_category(ozone_aqi) }} as ozone_aqi_category,
            {{ assign_aqi_category(no2_aqi) }} as no2_aqi_category
        from joined
    )

select *
from categories_added
