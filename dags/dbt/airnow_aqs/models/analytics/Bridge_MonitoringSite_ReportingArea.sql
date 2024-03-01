with reporting_areas as (
    select reporting_area_key, reporting_area
    from {{ ref('Dim_ReportingArea') }}
),

monitoring_sites as (
    select monitoring_site_key, site_name
    from {{ ref('Dim_MonitoringSite') }}
),

joined as (
    select 
        monitoring_site_key, 
        reporting_area_key, 
        {{ source('staging', 'stg_monitoring_sites_to_reporting_areas') }}.ValidDate as valid_date
    from {{ source('staging', 'stg_monitoring_sites_to_reporting_areas') }}
    join reporting_areas
    on reporting_areas.reporting_area = {{ source('staging', 'stg_monitoring_sites_to_reporting_areas') }}.ReportingAreaName
    join monitoring_sites
    on monitoring_sites.site_name = {{ source('staging', 'stg_monitoring_sites_to_reporting_areas') }}.SiteName

)

select * from joined