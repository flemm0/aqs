{% macro assign_aqi_category(aqi_value) %}

    {% set mapping = [
        ("Good", 0, 50),
        ("Moderate", 51, 100),
        ("Unhealthy for Sensitive Groups", 101, 150),
        ("Unhealthy", 151, 200),
        ("Very Unhealthy", 201, 300),
        ("Hazardous", 301, 500)
    ] %}

    {% if aqi %}
    case
        {% for category, lower, upper in mapping %}
            when {{ aqi }} >= {{ lower }} and {{ aqi }} <= {{ upper }} then '{{ category }}'
        {% endfor %}
        else 'Unknown'
    end
    {% else %}
    NULL
    {% endif %}

{% endmacro %}