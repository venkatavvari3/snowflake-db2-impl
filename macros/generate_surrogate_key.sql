-- Macro to generate surrogate keys
{% macro generate_surrogate_key(column_list) %}
    md5(
        concat(
            {% for column in column_list %}
                coalesce(cast({{ column }} as string), '')
                {%- if not loop.last -%}||{%- endif -%}
            {% endfor %}
        )
    )
{% endmacro %}
