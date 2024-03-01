with
    date_extracted as (
        select distinct
            strftime(strptime(ValidDate, '%m/%d/%Y'), '%Y%m%d') as date_key,
            date_trunc('day', strptime(ValidDate, '%m/%d/%Y')) as full_date,
            dayname(full_date) as day_name
        from {{ source("staging", "stg_hourly_data") }}
        order by full_date desc
    ),

    new_columns_added as (
        select
            date_key,
            full_date,
            strftime(full_date, '%a, %-d %B %Y') as date_name,
            day_name as day_of_week,
            {{ day_name_to_number(day_name) }} as day_number_of_week,
            extract('day' from full_date) as day_of_month,
            date_diff(
                'day',
                date_trunc('year', full_date),
                full_date
            )
            + 1 as day_of_year,
            {{ weekday_weekend(day_name) }} as weekday_weekend,
            date_diff(
                'day',
                date_trunc('year', full_date),
                full_date
            ) as week_of_year,
            monthname(full_date) as month_name,
            extract('month' from full_date) as month_of_year,
            case
                when
                    date_part('month', full_date + interval '1 day')
                    <> date_part('month', full_date)
                then 'yes'
                else 'no'
            end as is_last_day_of_month,
            date_part('quarter', full_date) as calendar_quarter,
            extract('year' from full_date) as calendar_year
        from date_extracted
    )

select *
from new_columns_added
