{% materialization distributed_table, adapter='clickhouse' %}
  {% set insert_distributed_sync = run_query("SELECT value FROM system.settings WHERE name = 'insert_distributed_sync'")[0][0] %}
  {% if insert_distributed_sync != '1' %}
     {% do exceptions.raise_compiler_error('To use distributed materialization setting insert_distributed_sync should be set to 1') %}
  {% endif %}

  {%- set local_suffix = adapter.get_clickhouse_local_suffix() -%}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}

  {% set existing_relation_local = existing_relation.incorporate(path={"identifier": model['name'] + local_suffix}) if existing_relation is not none else none %}
  {% set target_relation_local = target_relation.incorporate(path={"identifier": model['name'] + local_suffix}) if target_relation is not none else none %}

  {%- set backup_relation = none -%}
  {%- set preexisting_backup_relation = none -%}
  {%- set preexisting_intermediate_relation = none -%}

  {% if existing_relation_local is not none %}
    {%- set backup_relation_type = existing_relation_local.type -%}
    {%- set backup_relation = make_backup_relation(target_relation_local, backup_relation_type) -%}
    {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}
    {% if not existing_relation.can_exchange %}
      {%- set intermediate_relation =  make_intermediate_relation(target_relation_local) -%}
      {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation) -%}
    {% endif %}
  {% endif %}
  {% set view_relation = default__make_temp_relation(target_relation, '__dbt_tmp') %}
  -- drop the temp relations if they exist already in the database
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}
  {{ drop_relation_if_exists(view_relation) }}

  {% set grant_config = config.get('grants') %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {% call statement('main') %}
    {{ create_view_as(view_relation, sql) }}
  {% endcall %}

  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% if backup_relation is none %}
    {{ create_distributed_local_table(target_relation, target_relation_local, view_relation) }}
  {% elif existing_relation.can_exchange %}
    -- We can do an atomic exchange, so no need for an intermediate
    {% call statement('main') -%}
      {% do run_query(create_empty_table_from_relation(backup_relation, view_relation)) or '' %}
    {%- endcall %}
    {% do exchange_tables_atomic(backup_relation, existing_relation) %}
  {% else %}
    {% do run_query(create_empty_table_from_relation(intermediate_relation, view_relation)) or '' %}
    {{ adapter.rename_relation(existing_relation_local, backup_relation) }}
    {{ adapter.rename_relation(intermediate_relation, target_relation_local) }}
  {% endif %}
  {% do run_query(clickhouse__insert_into(target_relation, sql)) or '' %}
  {{ drop_relation_if_exists(view_relation) }}
  -- cleanup
  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}
  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ adapter.commit() }}
  {{ drop_relation_if_exists(backup_relation) }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}
  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}

{% macro create_distributed_table(relation, local_relation) %}
    {%- set cluster = adapter.get_clickhouse_cluster_name() -%}
   {% if cluster is none %}
        {% do exceptions.raise_compiler_error('Cluster name should be defined for using distributed materializations, current is None') %}
    {% endif %}

   {%- set cluster = cluster[1:-1] -%}
   {%- set sharding = config.get('sharding_key') -%}

    CREATE TABLE {{ relation }} {{ on_cluster_clause() }} AS {{ local_relation }}
    ENGINE = Distributed('{{ cluster}}', '{{ relation.schema }}', '{{ local_relation.name }}'
    {% if sharding is not none %}
        , {{ sharding }}
    {% endif %}
    )
 {% endmacro %}

{% macro create_empty_table_from_relation(relation, source_relation) -%}
  {%- set sql_header = config.get('sql_header', none) -%}
  {%- set columns = adapter.get_columns_in_relation(source_relation) | list -%}

  {%- set col_list = [] -%}
  {% for col in columns %}
    {{col_list.append(col.name + ' ' + col.data_type) or '' }}
  {% endfor %}
  {{ sql_header if sql_header is not none }}

  create table {{ relation.include(database=False) }}
  {{ on_cluster_clause() }} (
      {{col_list | join(', ')}}
  )

  {{ engine_clause() }}
  {{ order_cols(label="order by") }}
  {{ primary_key_clause(label="primary key") }}
  {{ partition_cols(label="partition by") }}
  {{ adapter.get_model_settings(model) }}
{%- endmacro %}

{% macro create_distributed_local_table(distributed_relation, shard_relation, structure_relation, sql_query=none) -%}
  {{ drop_relation_if_exists(shard_relation) }}
  {{ drop_relation_if_exists(distributed_relation) }}
  {% do run_query(create_empty_table_from_relation(shard_relation, structure_relation)) or '' %}
  {% do run_query(create_distributed_table(distributed_relation, shard_relation)) or '' %}
  {% if sql_query is not none %}
    {% do run_query(clickhouse__insert_into(distributed_relation, sql_query)) or '' %}
  {% endif %}
{%- endmacro %}
