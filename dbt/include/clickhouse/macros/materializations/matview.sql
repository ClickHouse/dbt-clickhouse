{% macro clickhouse__create_matview_as(relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  {%- set engine = config.get('engine', default='MergeTree()') -%}

  {%- set target_table = config.get('target_table', default=None) -%}

  create materialized view
  {{ relation.include(database=False) }}
  {{ on_cluster_clause()}}
  {% if target_table is not none %}
    to {{ target_table }}
  {% else %}
    {{ engine_clause() }}
    {{ order_cols(label="order by") }}
    {{ primary_key_clause(label="primary key") }}
    {{ partition_cols(label="partition by") }}
  {% endif %}
  {{ adapter.get_model_settings(model) }}
  as (
    {{ sql }}
  )
{%- endmacro %}
