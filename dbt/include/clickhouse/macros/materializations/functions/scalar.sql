{% macro clickhouse__formatted_scalar_function_args_sql() %}
    {% set args = [] %}
    {% for arg in model.arguments -%}
      {%- do args.append(arg.name) -%}
    {%- endfor %}
    {{ args | join(', ') }}
{% endmacro %}

{% macro clickhouse__scalar_function_create_replace_signature_sql(target_relation) %}
  CREATE OR REPLACE FUNCTION {{ target_relation.include(database=false, schema=false) }} {{ on_cluster_clause(this) }} AS ({{ clickhouse__formatted_scalar_function_args_sql() }}) ->
{% endmacro %}

{% macro clickhouse__scalar_function_body_sql() %}
  {{ model.compiled_code }}
{% endmacro %}

{% macro clickhouse__scalar_function_sql(target_relation) %}
  {{ clickhouse__scalar_function_create_replace_signature_sql(target_relation) }}
  {{ clickhouse__scalar_function_body_sql() }};
{% endmacro %}
