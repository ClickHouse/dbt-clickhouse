{% materialization table, adapter='clickhouse' %}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}
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
    {{ log('Creating new relation ' + target_relation.name )}}
    -- There is not existing relation, so we can just create
    {% call statement('main') -%}
      {{ get_create_table_as_sql(False, target_relation, sql) }}
    {%- endcall %}
  {% elif existing_relation.can_exchange %}
    -- We can do an atomic exchange, so no need for an intermediate
    {% call statement('main') -%}
      {{ get_create_table_as_sql(False, backup_relation, sql) }}
    {%- endcall %}
    {% do exchange_tables_atomic(backup_relation, existing_relation) %}
  {% else %}
    -- We have to use an intermediate and rename accordingly
    {% call statement('main') -%}
      {{ get_create_table_as_sql(False, intermediate_relation, sql) }}
    {%- endcall %}
    {{ adapter.rename_relation(existing_relation, backup_relation) }}
    {{ adapter.rename_relation(intermediate_relation, target_relation) }}
  {% endif %}

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

{% macro engine_clause() %}
  {%- set is_cluster = adapter.is_clickhouse_cluster_mode() -%}
  {% if is_cluster -%}
    engine = {{ 'Replicated' ~ config.get('engine', default='MergeTree()') | replace('MergeTree(', 'MergeTree(\'' ~ config.get('replica_path', default='/clickhouse/tables/{database}/{table}/{shard}/{uuid}')  ~ '\', \'{replica}\'') }}
  {% else %}
    engine = {{ config.get('engine', default='MergeTree()') }}
  {%- endif %}
{%- endmacro -%}

{% macro partition_cols(label) %}
  {%- set cols = config.get('partition_by', validator=validation.any[list, basestring]) -%}
  {%- if cols is not none %}
    {%- if cols is string -%}
      {%- set cols = [cols] -%}
    {%- endif -%}
    {{ label }} (
    {%- for item in cols -%}
      {{ item }}
      {%- if not loop.last -%},{%- endif -%}
    {%- endfor -%}
    )
  {%- endif %}
{%- endmacro -%}

{% macro primary_key_clause(label) %}
  {%- set primary_key = config.get('primary_key', validator=validation.any[basestring]) -%}

  {%- if primary_key is not none %}
    {{ label }} {{ primary_key }}
  {%- endif %}
{%- endmacro -%}

{% macro ttl_clause(label) %}
  {%- set ttl = config.get('ttl', validator=validation.any[basestring]) -%}

  {%- if ttl is not none %}
    {{ label }} {{ ttl }}
  {%- endif %}
{%- endmacro -%}

{% macro order_cols(label) %}
  {%- set cols = config.get('order_by', validator=validation.any[list, basestring]) -%}
  {%- set engine = config.get('engine', default='MergeTree()') -%}
  {%- set supported = [
    'HDFS',
    'MaterializedPostgreSQL',
    'OSS',
    'S3',
    'EmbeddedRocksDB',
    'Hive'
  ] -%}

  {%- if 'MergeTree' in engine or engine in supported %}
    {%- if cols is not none %}
      {%- if cols is string -%}
        {%- set cols = [cols] -%}
      {%- endif -%}
      {{ label }} (
      {%- for item in cols -%}
        {{ item }}
        {%- if not loop.last -%},{%- endif -%}
      {%- endfor -%}
      )
    {%- else %}
      {{ label }} (tuple())
    {%- endif %}
  {%- endif %}
{%- endmacro -%}

{% macro on_cluster_clause(label) %}
  {% set active_cluster = adapter.get_clickhouse_cluster_name() %}
  {%- if active_cluster is not none %}
    ON CLUSTER {{ active_cluster }}
  {%- endif %}
{%- endmacro -%}

{% macro clickhouse__create_table_as(temporary, relation, sql) -%}
    {% set create_table = create_table_or_empty(temporary, relation, sql) %}
    {% if adapter.is_before_version('22.7.1.2484') -%}
        {{ create_table }}
    {%- else %}
        {% call statement('create_table_empty') %}
            {{ create_table }}
        {% endcall %}

        {%- set is_cluster = adapter.is_clickhouse_cluster_mode() -%}
        {% if is_cluster -%}
          {% call statement('create_distributed_table') %}
              {{ create_distributed_table(relation) }}
          {% endcall %}
        {%- endif %}

        {{ clickhouse__insert_into(relation.include(database=False), sql) }}
    {%- endif %}
{%- endmacro %}

{% macro create_table_or_empty(temporary, relation, sql) -%}
    {%- set sql_header = config.get('sql_header', none) -%}

    {{ sql_header if sql_header is not none }}

    {%- set sharding_key = config.get('sharding_key', 'rand()') -%}
    {%- set is_cluster = adapter.is_clickhouse_cluster_mode() -%}

    {% if temporary -%}
        create temporary table {{ relation.name }}{% if is_cluster -%}_local{%- endif %}
        engine Memory
        {{ order_cols(label="order by") }}
        {{ partition_cols(label="partition by") }}
        {{ ttl_clause(label="ttl") }}
        {{ adapter.get_model_settings(model) }}
    {%- else %}
        create table {{ relation.include(database=False) }}{% if is_cluster -%}_local{%- endif %}
        {{ on_cluster_clause()}}
        {{ engine_clause() }}
        {{ order_cols(label="order by") }}
        {{ primary_key_clause(label="primary key") }}
        {{ partition_cols(label="partition by") }}
        {{ ttl_clause(label="ttl") }}
        {{ adapter.get_model_settings(model) }}
        {% if not adapter.is_before_version('22.7.1.2484') -%}
            empty
        {%- endif %}
    {%- endif %}
    as (
        {{ sql }}
    )
{%- endmacro %}

{% macro create_distributed_table(relation) -%}
    {%- set sharding_key = config.get('sharding_key', 'rand()') -%}

    create table if not exists {{ relation.include(database=False) }} {{ on_cluster_clause()}} as {{ relation.include(database=False) }}_local
      engine = Distributed({{ adapter.get_clickhouse_cluster_name() }}, {{ relation.schema }}, {{ relation.identifier }}_local, sipHash64({{ sharding_key }}))
{%- endmacro %}

{% macro clickhouse__insert_into(target_relation, sql) %}
  {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
  {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}

  insert into {{ target_relation }} ({{ dest_cols_csv }})
  {{ sql }}
{%- endmacro %}
