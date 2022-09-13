{% macro clickhouse__any_value(expression) -%}
    any({{ expression }})
{%- endmacro %}


{% macro clickhouse__bool_or(expression) -%}
    any({{ expression }}) > 0
{%- endmacro %}


{% macro clickhouse__cast_bool_to_text(field) %}
    multiIf({{ field }} > 0, 'true', {{ field }} = 0, 'false', NULL)
{% endmacro %}