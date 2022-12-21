{% macro clickhouse_s3table(url, format, structure) %}
  {{ log("STARTING S3") }}
  {%- if not format -%}
    {{ log("DID WE GET TO BAD FORMAT") }}
    {%- set format = config.get('s3_format') -%}
    {%- if format is none -%}
        {{ exceptions.raise_compiler_error('Format required for S3table query') }}
    {%- endif -%}
  {%- endif -%}

  {%- if not url -%}
   {{ log("DID WE GET TO BAD URL") }}
    {%- set url = config.get('s3_url') -%}
    {%- if url is none -%}
        {{ exceptions.raise_compiler_error('Url required for S3table query') }}
    {%- endif -%}
  {%- endif -%}

  {%- if not structure -%}
    {%- set structure = config.get('s3_structure') -%}
  {%- endif -%}
  {{ adapter.s3table_clause(url, format, structure) }}
{% endmacro %}