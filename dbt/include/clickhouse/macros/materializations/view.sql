{%- materialization view, adapter='clickhouse' -%}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='view') -%}

  {% set grant_config = config.get('grants') %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% if existing_relation is none %}
    {{ log('Creating new relation ' + target_relation.name )}}
  {% else %}
    {{ log('Relation ' + target_relation.name + ' already exists, replacing it' )}}
  {% endif %}

  {% call statement('main') -%}
    {{ get_create_view_as_sql(target_relation, sql) }}
  {%- endcall %}

  -- cleanup
  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization -%}


{% macro clickhouse__create_view_as(relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}
  {{ sql_header if sql_header is not none }}

  create or replace view {{ relation.include(database=False) }} {{ on_cluster_clause(relation)}}
    {% set contract_config = config.get('contract') %}
    {% if contract_config.enforced %}
      {{ get_assert_columns_equivalent(sql) }}
    {%- endif %}
  as (
    {{ sql }}
    {{ adapter.get_model_query_settings(model) }}
  )
      {% if model.get('config').get('materialized') == 'view' %}
      {{ adapter.get_model_settings(model, config.get('engine', default='MergeTree')) }}
    {%- endif %}

{%- endmacro %}

