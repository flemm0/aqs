{% macro day_name_to_number(day_name) %}
    {% set day_number = {
        "Monday": "Weekday",
        "Tuesday": "Weekday",
        "Wednesday": "Weekday",
        "Thursday": "Weekday",
        "Friday": "Weekday",
        "Saturday": "Weekend",
        "Sunday": "Weekend"
    } %}

    {% if day_name in day_number %}
        {{ day_number[day_name] }}
    {% else %}
        NULL
    {% endif %}
{% endmacro %}