with columns_renamed_nulls_coalesced as (
    select
        reporting_area,
        state_code,
        coalesce( country_code, 'Missing' ) as country_code,
        forecasts,
        coalesce( action_day_name, 'None' ) as action_day_name,
        latitude,
        longitude,
        coalesce( gmt_offset, 9999999999 ) as gmt_offset,
        twc_code,
        usa_today as reports_to_usa_today,
        coalesce( forecast_source, 'Missing' ) as forecast_source,
        ValidDate as valid_date
    from {{ source('staging', 'stg_reporting_areas') }}
),

reporting_area_ids as (
    select 
        distinct
        ReportingAreaName as reporting_area, 
        ReportingAreaID as reporting_area_id
    from {{ source('staging', 'stg_monitoring_sites_to_reporting_areas') }}
),

reporting_area_id_added as (
    select reporting_area_ids.reporting_area_id, columns_renamed_nulls_coalesced.*
    from columns_renamed_nulls_coalesced
    join reporting_area_ids
    on columns_renamed_nulls_coalesced.reporting_area = reporting_area_ids.reporting_area
),

surrogate_key_added as (
    select md5( concat(reporting_area, reporting_area_id) ) as reporting_area_key, *
    from reporting_area_id_added
)

select * from surrogate_key_added