{#-
  Create or update a materialized view in ClickHouse.
  This involves creating both the materialized view itself and a
  target table that the materialized view writes to.
-#}
{%- materialization materialized_view, adapter='clickhouse' -%}

  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set mv_relation = target_relation.derivative('_mv', 'materialized_view') -%}
  {%- set cluster_clause = on_cluster_clause(target_relation) -%}

  {# look for an existing relation for the target table and create backup relations if necessary #}
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set backup_relation = none -%}
  {%- set preexisting_backup_relation = none -%}
  {%- set preexisting_intermediate_relation = none -%}
  {% if existing_relation is not none %}
    {%- set backup_relation_type = existing_relation.type -%}
    {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
    {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
    {% if not existing_relation.can_exchange %}
      {%- set intermediate_relation =  make_intermediate_relation(target_relation) -%}
      {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation) -%}
    {% endif %}
  {% endif %}

  {% set grant_config = config.get('grants') %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- drop the temp relations if they exist already in the database
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% if backup_relation is none %}
    {{ log('Creating new materialized view ' + target_relation.name )}}
    {% call statement('main') -%}
      {{ clickhouse__get_create_materialized_view_as_sql(target_relation, sql) }}
    {%- endcall %}
  {% elif existing_relation.can_exchange %}
    {{ log('Replacing existing materialized view ' + target_relation.name) }}
    {% call statement('drop existing materialized view') %}
      drop view if exists {{ mv_relation }} {{ cluster_clause }}
    {% endcall %}
    {% if should_full_refresh() %}
      {% call statement('main') -%}
        {{ get_create_table_as_sql(False, backup_relation, sql) }}
      {%- endcall %}
      {% do exchange_tables_atomic(backup_relation, existing_relation) %}
    {% else %}
      -- we need to have a 'main' statement
      {% call statement('main') -%}
        select 1
      {%- endcall %}
    {% endif %}
    {% call statement('create new materialized view') %}
      {{ clickhouse__create_mv_sql(mv_relation, existing_relation, cluster_clause, sql) }}
    {% endcall %}
  {% else %}
    {{ log('Replacing existing materialized view ' + target_relation.name) }}
    {{ clickhouse__replace_mv(target_relation, existing_relation, intermediate_relation, backup_relation, sql) }}
  {% endif %}

  -- cleanup
  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ adapter.commit() }}

  {{ drop_relation_if_exists(backup_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation, mv_relation]}) }}

{%- endmaterialization -%}


{#
  There are two steps to creating a materialized view:
  1. Create a new table based on the SQL in the model
  2. Create a materialized view using the SQL in the model that inserts
  data into the table creating during step 1
#}
{% macro clickhouse__get_create_materialized_view_as_sql(relation, sql) -%}
  {% call statement('create_target_table') %}
    {{ get_create_table_as_sql(False, relation, sql) }}
  {% endcall %}
  {%- set cluster_clause = on_cluster_clause(relation) -%}
  {%- set mv_relation = relation.derivative('_mv', 'materialized_view') -%}
  {{ clickhouse__create_mv_sql(mv_relation, relation, cluster_clause, sql) }}
{%- endmacro %}


{% macro clickhouse__create_mv_sql(mv_relation, target_table, cluster_clause, sql)  -%}
  create materialized view if not exists {{ mv_relation }} {{ cluster_clause }}
  to {{ target_table }}
  as {{ sql }}
{%- endmacro %}


{% macro clickhouse__replace_mv(target_relation, existing_relation, intermediate_relation, backup_relation, sql) %}
  {# drop existing materialized view while we recreate the target table #}
  {%- set cluster_clause = on_cluster_clause(target_relation) -%}
  {%- set mv_relation = target_relation.derivative('_mv', 'materialized_view') -%}
  {% call statement('drop existing mv') -%}
    drop view if exists {{ mv_relation }} {{ cluster_clause }}
  {%- endcall %}

  {# recreate the target table #}
  {% call statement('main') -%}
    {{ get_create_table_as_sql(False, intermediate_relation, sql) }}
  {%- endcall %}
  {{ adapter.rename_relation(existing_relation, backup_relation) }}
  {{ adapter.rename_relation(intermediate_relation, target_relation) }}

  {# now that the target table is recreated, we can finally create our new view #}
  {{ clickhouse__create_mv_sql(mv_relation, target_relation, cluster_clause, sql) }}
{% endmacro %}
