{% macro clickhouse__create_view_as(relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  create view {{ relation.include(database=False) }} {{ on_cluster_clause()}}
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
    create database if not exists {{ relation.without_identifier().include(database=False) }}
        {{ on_cluster_clause()}}
        {{ adapter.clickhouse_db_engine_clause() }}
  {% endcall %}
{% endmacro %}

{% macro clickhouse__drop_schema(relation) -%}
  {%- call statement('drop_schema') -%}
    drop database if exists {{ relation.without_identifier().include(database=False) }} {{ on_cluster_clause()}}
  {%- endcall -%}
{% endmacro %}

{% macro clickhouse__list_relations_without_caching(schema_relation) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    select
      t.name as name,
      t.database as schema,
      if(engine not in ('MaterializedView', 'View'), 'table', 'view') as type,
      db.engine as db_engine
    from system.tables as t JOIN system.databases as db on t.database = db.name
    where schema = '{{ schema_relation.schema }}'
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}

{% macro clickhouse__get_columns_in_relation(relation) -%}
  {% call statement('get_columns', fetch_result=True) %}
    select name, type from system.columns where table = '{{ relation.identifier }}'
    {% if relation.schema %}
      and database = '{{ relation.schema }}'
    {% endif %}
    order by position
  {% endcall %}
  {{ return(sql_convert_columns_in_relation(load_result('get_columns').table)) }}
{% endmacro %}

{% macro clickhouse__drop_relation(relation, obj_type='table') -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    drop {{ obj_type }} if exists {{ relation }} {{ on_cluster_clause()}}
  {%- endcall %}
{% endmacro %}

{% macro clickhouse__rename_relation(from_relation, to_relation, obj_type='table') -%}
  {% call statement('drop_relation') %}
    drop {{ obj_type }} if exists {{ to_relation }} {{ on_cluster_clause()}}
  {% endcall %}
  {% call statement('rename_relation') %}
    rename {{ obj_type }} {{ from_relation }} to {{ to_relation }} {{ on_cluster_clause()}}
  {% endcall %}
{% endmacro %}

{% macro clickhouse__truncate_relation(relation) -%}
  {% call statement('truncate_relation') -%}
    truncate table {{ relation }} {{ on_cluster_clause()}}
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
    alter table {{ relation }} {{ on_cluster_clause()}} modify column {{ adapter.quote(column_name) }} {{ new_column_type }}
  {% endcall %}
{% endmacro %}

{% macro exchange_tables_atomic(old_relation, target_relation, obj_types='TABLES') %}

  {%- if adapter.get_clickhouse_cluster_name() is not none and obj_types == 'TABLES' %}
    {% do run_query("SYSTEM SYNC REPLICA " + on_cluster_clause() + target_relation.schema + '.' + target_relation.identifier) %}
  {%- endif %}
  
  {%- call statement('exchange_tables_atomic') -%}
    EXCHANGE {{ obj_types }} {{ old_relation }} AND {{ target_relation }} {{ on_cluster_clause()}}
  {% endcall %}
{% endmacro %}
