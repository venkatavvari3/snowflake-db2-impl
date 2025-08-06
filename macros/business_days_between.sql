-- Macro to calculate business days between two dates
{% macro business_days_between(start_date, end_date) %}
    (
        datediff('day', {{ start_date }}, {{ end_date }}) -
        (datediff('week', {{ start_date }}, {{ end_date }}) * 2) -
        case when dayofweek({{ start_date }}) = 1 then 1 else 0 end -
        case when dayofweek({{ end_date }}) = 7 then 1 else 0 end
    )
{% endmacro %}
