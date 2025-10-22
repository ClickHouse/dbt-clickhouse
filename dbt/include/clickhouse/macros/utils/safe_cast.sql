-- This macro allows you to set a default value for columns based on their type.

{% macro clickhouse__safe_cast(field, dtype) %}
  {%- if field == 'null' -%}
    CAST(defaultValueOfTypeName('{{ dtype }}') AS {{ dtype }})
  {%- else -%}
    CAST({{ field }} AS {{ dtype }})
  {%- endif -%}
{% endmacro %}
