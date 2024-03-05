-- Define variable to store target database

{%set target_name = target.name %}

with
    date_extracted as (
        select distinct

            {% if target_name == 'prod' %}
            to_char(to_date(ValidDate, 'MM/DD/YYYY'), 'YYYYMMDD') as date_key,
            {% else %}
            strftime(strptime(ValidDate, '%m/%d/%Y'), '%Y%m%d') as date_key,
            {% endif %}

            {% if target_name == 'prod' %}
            to_date(ValidDate, 'MM/DD/YYYY') as full_date,
            {% else %}
            date_trunc('day', strptime(ValidDate, '%m/%d/%Y')) as full_date,
            {% endif %}

            {% if target_name == 'prod' %}
            decode(dayname(full_date), 'Mon','Monday', 'Tues','Tuesday', 'Wed','Wednesday', 'Thu','Thursday', 'Fri','Friday', 'Sat','Saturday', 'Sun', 'Sunday') as day_name
            {% else %}
            dayname(full_date) as day_name
            {% endif %}

        from {{ source("staging", "stg_hourly_data") }}
        order by full_date desc
    ),

    new_columns_added as (
        select
            date_key,
            full_date,

            {% if target_name == 'prod' %}
            concat(dayname(full_date), ' ', to_char(full_date, 'Mon DD, YYYY')) AS date_name,
            {% else %}
            strftime(full_date, '%a, %-d %B %Y') as date_name,
            {% endif %}

            day_name as day_of_week,

            {% if target_name == 'prod' %}
            dayofweek(full_date) as day_number_of_week,
            {% else %}
            {{ day_name_to_number(day_name) }} as day_number_of_week,
            {% endif %}

            {% if target_name == 'prod' %}
            extract('day', full_date) as day_of_month,
            {% else %}
            extract('day' from full_date) as day_of_month,
            {% endif %}

            {% if target_name == 'prod' %}
            dayofyear(full_date) as day_of_year,
            {% else %}
            date_diff(
                'day',
                date_trunc('year', full_date),
                full_date
            )
            + 1 as day_of_year,
            {% endif %}

            {{ weekday_weekend(day_name) }} as weekday_weekend,

            {% if target_name == 'prod' %}
            weekofyear(full_date) as week_of_year,
            {% else %}
            date_diff(
                'day',
                date_trunc('year', full_date),
                full_date
            ) as week_of_year,
            {% endif %}

            monthname(full_date) as month_name,

            {% if target_name == 'prod' %}
            extract('month', full_date) as month_of_year,
            {% else %}
            extract('month' from full_date) as month_of_year,
            {% endif %}

            {% if target_name == 'prod' %}
            case
                when
                    last_day(full_date, 'month') = full_date
                then 'yes'
                else 'no'
            end as is_last_day_of_month,
            {% else %}
            case
                when
                    date_part('month', full_date + interval '1 day')
                    <> date_part('month', full_date)
                then 'yes'
                else 'no'
            end as is_last_day_of_month,
            {% endif %}

            date_part('quarter', full_date) as calendar_quarter,

            {% if target_name == 'prod' %}
            year(full_date) as calendar_year
            {% else %}
            extract('year' from full_date) as calendar_year
            {% endif %}
            
        from date_extracted
    )

select *
from new_columns_added
