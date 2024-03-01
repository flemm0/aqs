{% macro weekday_weekend(day_name) %}
    {% set day_number = {
        "Monday": 1,
        "Tuesday": 2,
        "Wednesday": 3,
        "Thursday": 4,
        "Friday": 5,
        "Saturday": 6,
        "Sunday": 7
    } %}

    {% if day_name in day_number %}
        {{ day_number[day_name] }}
    {% else %}
        NULL
    {% endif %}
{% endmacro %}