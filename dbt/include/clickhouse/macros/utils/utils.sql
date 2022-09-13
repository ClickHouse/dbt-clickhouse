{% macro clickhouse__any_value(expression) -%}
    any({{ expression }})
{%- endmacro %}


{% macro clickhouse__bool_or(expression) -%}
    any({{ expression }}) > 0
{%- endmacro %}


{% macro clickhouse__cast_bool_to_text(field) %}
    multiIf({{ field }} > 0, 'true', {{ field }} = 0, 'false', NULL)
{% endmacro %}


{% macro clickhouse__hash(field) -%}
    lower(hex(MD5(toString({{ field }} ))))
{%- endmacro %}


{%- macro clickhouse__last_day(date, datepart) -%}
    {{ dbt.dateadd('day', '-1', dbt.dateadd(datepart, '1', dbt.date_trunc(datepart, date)))}}
{%- endmacro -%}