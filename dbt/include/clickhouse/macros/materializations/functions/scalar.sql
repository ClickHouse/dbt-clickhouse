{% macro clickhouse__formatted_scalar_function_args_sql() -%}
    {{ model.arguments | map(attribute='name') | join(', ') }}
{%- endmacro %}

{% macro clickhouse__scalar_function_create_replace_signature_sql(target_relation) -%}
  CREATE OR REPLACE FUNCTION {{ target_relation.include(database=false, schema=false) }} {{ on_cluster_clause(this) }} AS ({{ formatted_scalar_function_args_sql() }}) ->
{%- endmacro %}

{% macro clickhouse__scalar_function_body_sql() -%}
  {{ model.compiled_code }}
{%- endmacro %}
