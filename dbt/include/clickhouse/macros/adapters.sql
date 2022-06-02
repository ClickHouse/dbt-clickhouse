{% macro engine_clause(label) %}
  {%- set engine = config.get('engine', validator=validation.any[basestring]) -%}
  {%- if engine is not none %}
    {{ label }} = {{ engine }}
  {%- else %}
    {{ label }} = MergeTree()
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

{% macro order_cols(label) %}
  {%- set cols = config.get('order_by', validator=validation.any[list, basestring]) -%}
  {%- set engine = config.get('engine', validator=validation.any[basestring]) -%}
  {%- set supported = [
    'HDFS',
    'MaterializedPostgreSQL',
    'S3',
    'EmbeddedRocksDB',
    'Hive'
  ] -%}

  {%- if engine is none or 'MergeTree' in engine or engine in supported %}
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
  {% set on_cluster = adapter.get_clickhouse_cluster_name() %}
  {%- if on_cluster is not none %}
    {{ label }} {{ on_cluster }}
  {%- endif %}
{%- endmacro -%}

{% macro clickhouse__create_table_as(temporary, relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  {% if temporary -%}
    create temporary table {{ relation.name }}
    engine = Memory
    {{ order_cols(label="order by") }}
    {{ partition_cols(label="partition by") }}
  {%- else %}
    create table {{ relation.include(database=False) }}
    {{ on_cluster_clause(label="on cluster") }}
    {{ engine_clause(label="engine") }}
    {{ order_cols(label="order by") }}
    {{ primary_key_clause(label="primary key") }}
    {{ partition_cols(label="partition by") }}
  {%- endif %}
  as (
    {{ sql }}
  )
{%- endmacro %}

{% macro clickhouse__create_view_as(relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  create view {{ relation.include(database=False) }} {{ on_cluster_clause(label="on cluster") }}
  as (
    {{ sql }}
  )
{%- endmacro %}

{% macro clickhouse__list_schemas(database) %}
  {% call statement('list_schemas', fetch_result=True, auto_begin=False) %}
    select name from system.databases
  {% endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}

{% macro clickhouse__create_schema(relation) -%}
  {%- call statement('create_schema') -%}
    create database if not exists {{ relation.without_identifier().include(database=False) }} {{ on_cluster_clause(label="on cluster") }}
  {% endcall %}
{% endmacro %}

{% macro clickhouse__drop_schema(relation) -%}
  {%- call statement('drop_schema') -%}
    drop database if exists {{ relation.without_identifier().include(database=False) }} {{ on_cluster_clause(label="on cluster") }}
  {%- endcall -%}
{% endmacro %}

{% macro clickhouse__list_relations_without_caching(schema_relation) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    select
      null as db,
      name as name,
      database as schema,
      if(engine not in ('MaterializedView', 'View'), 'table', 'view') as type
    from system.tables as t
    where schema = '{{ schema_relation.schema }}'
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}

{% macro clickhouse__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
    select
      name,
      type,
      position
    from system.columns
    where
      table = '{{ relation.identifier }}'
    {% if relation.schema %}
      and database = '{{ relation.schema }}'
    {% endif %}
    order by position
  {% endcall %}
  {% do return(load_result('get_columns_in_relation').table) %}
{% endmacro %}

{% macro clickhouse__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    drop table if exists {{ relation }} {{ on_cluster_clause(label="on cluster") }}
  {%- endcall %}
{% endmacro %}

{% macro clickhouse__rename_relation(from_relation, to_relation) -%}
  {% call statement('drop_relation') %}
    drop table if exists {{ to_relation }} {{ on_cluster_clause(label="on cluster") }}
  {% endcall %}
  {% call statement('rename_relation') %}
    rename table {{ from_relation }} to {{ to_relation }} {{ on_cluster_clause(label="on cluster") }}
  {% endcall %}
{% endmacro %}

{% macro clickhouse__truncate_relation(relation) -%}
  {% call statement('truncate_relation') -%}
    truncate table {{ relation }}
  {%- endcall %}
{% endmacro %}

{% macro clickhouse__make_temp_relation(base_relation, suffix) %}
  {% set tmp_identifier = base_relation.identifier ~ suffix %}
  {% set tmp_relation = base_relation.incorporate(
                              path={"identifier": tmp_identifier, "schema": None}) -%}
  {% do return(tmp_relation) %}
{% endmacro %}


{% macro clickhouse__generate_database_name(custom_database_name=none, node=none) -%}
  {% do return(None) %}
{%- endmacro %}

{% macro clickhouse__current_timestamp() -%}
  now()
{%- endmacro %}

{% macro clickhouse__get_columns_in_query(select_sql) %}
  {% call statement('get_columns_in_query', fetch_result=True, auto_begin=False) -%}
    select * from (
        {{ select_sql }}
    ) as __dbt_sbq
    limit 0
  {% endcall %}

  {{ return(load_result('get_columns_in_query').table.columns | map(attribute='name') | list) }}
{% endmacro %}

{% macro clickhouse__alter_column_type(relation, column_name, new_column_type) -%}
  {% call statement('alter_column_type') %}
    alter table {{ relation }} {{ on_cluster_clause(label="on cluster") }} modify column {{ adapter.quote(column_name) }} {{ new_column_type }}
  {% endcall %}
{% endmacro %}
