{% macro clickhouse__aggregate_function_sql(target_relation) -%}
    {#- aggregate functions dont need special handling in clickhouse, so we can reuse the scalar approach -#}
    {{ scalar_function_sql(target_relation) }}
{%- endmacro %}
