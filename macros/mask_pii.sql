-- Macro to mask PII data in non-production environments
{% macro mask_pii(column_name, mask_value='***MASKED***') %}
    {% if var('mask_pii', false) %}
        '{{ mask_value }}'
    {% else %}
        {{ column_name }}
    {% endif %}
{% endmacro %}
