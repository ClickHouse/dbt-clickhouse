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
  engine = {{ config.get('engine', default='MergeTree()') }}
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

{% macro order_cols(label) %}
  {%- set cols = config.get('order_by', validator=validation.any[list, basestring]) -%}
  {%- set engine = config.get('engine', default='MergeTree()') -%}
  {%- set supported = [
    'HDFS',
    'MaterializedPostgreSQL',
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

{% macro ttl_config(label) %}
  {%- if config.get("ttl")%}
    {{ label }} {{ config.get("ttl") }}
  {%- endif %}
{%- endmacro -%}

{% macro on_cluster_clause(relation, force_sync) %}
  {% set active_cluster = adapter.get_clickhouse_cluster_name() %}
  {%- if active_cluster is not none and relation.should_on_cluster %}
    {# Add trailing whitespace to avoid problems when this clause is not last #}
    ON CLUSTER {{ active_cluster + ' ' }}
    {%- if force_sync %}
    SYNC
    {%- endif %}
  {%- endif %}
{%- endmacro -%}

{% macro clickhouse__create_table_as(temporary, relation, sql) -%}
    {% set has_contract = config.get('contract').enforced %}
    {% set create_table = create_table_or_empty(temporary, relation, sql, has_contract) %}
    {% if adapter.is_before_version('22.7.1.2484') or temporary -%}
        {{ create_table }}
    {%- else %}
        {% call statement('create_table_empty') %}
            {{ create_table }}
        {% endcall %}
         {% if config.get('projections')%}
                {{ projection_statement(relation) }}
         {% endif %}


        {{ clickhouse__insert_into(relation, sql, has_contract) }}
    {%- endif %}
{%- endmacro %}

{% macro projection_statement(relation) %}
    {%- set projections = config.get('projections', default=[]) -%}

    {%- for projection in projections %}
         {% call statement('add_projections') %}
                ALTER TABLE {{ relation }} ADD PROJECTION {{ projection.get('name') }}
        (
            {{ projection.get('query') }}
        )
            {%endcall  %}
    {%- endfor %}
{%- endmacro %}

{% macro create_table_or_empty(temporary, relation, sql, has_contract) -%}
    {%- set sql_header = config.get('sql_header', none) -%}

    {{ sql_header if sql_header is not none }}

    {% if temporary -%}
        create temporary table {{ relation }}
        engine Memory
        {{ adapter.get_model_settings(model) }}
        as (
          {{ sql }}
        )
    {%- else %}
        create table {{ relation }}
        {{ on_cluster_clause(relation)}}
        {%- if has_contract%}
          {{ get_assert_columns_equivalent(sql) }}
          {{ get_table_columns_and_constraints() }}
        {%- endif %}
        {{ engine_clause() }}
        {{ order_cols(label="order by") }}
        {{ primary_key_clause(label="primary key") }}
        {{ partition_cols(label="partition by") }}
        {{ ttl_config(label="ttl")}}
        {{ adapter.get_model_settings(model) }}

        {%- if not has_contract %}
          {%- if not adapter.is_before_version('22.7.1.2484') %}
            empty
          {%- endif %}
          as (
            {{ sql }}
          )
        {%- endif %}
    {%- endif %}

{%- endmacro %}

{% macro clickhouse__insert_into(target_relation, sql, has_contract) %}
  {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
  {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}

  insert into {{ target_relation }}
        ({{ dest_cols_csv }})
  {%- if has_contract -%}
    -- Use a subquery to get columns in the right order
          SELECT {{ dest_cols_csv }} FROM ( {{ sql }} )
  {%- else -%}
      {{ sql }}
  {%- endif -%}
  {{ adapter.get_model_query_settings(model) }}
{%- endmacro %}
