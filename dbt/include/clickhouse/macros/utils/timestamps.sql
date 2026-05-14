{% macro clickhouse__current_timestamp() -%}
  now()
{%- endmacro %}

{% macro clickhouse__snapshot_string_as_time(timestamp) -%}
  {%- set result = "toDateTime('" ~ timestamp ~ "')" -%}
  {{ return(result) }}
{%- endmacro %}

{%- macro clickhouse__convert_timezone(column, target_tz, source_tz="UTC") -%}
    toTimeZone(
        toDateTime({{ column }}, '{{ source_tz }}'),
        '{{ target_tz }}'
    )
{%- endmacro -%}
