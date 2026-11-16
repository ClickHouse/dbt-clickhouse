{% macro clickhouse__check_schema_exists(information_schema, schema) -%}
  {% set sql %}
    select count(*) from system.databases where name = '{{ schema }}'
  {% endset %}
  {{ return(run_query(sql)) }}
{% endmacro %}

{% macro clickhouse__list_schemas(database) %}
  {% call statement('list_schemas', fetch_result=True, auto_begin=False) %}
    select name from system.databases
  {% endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}

{% macro clickhouse__create_schema(relation) -%}
  {%- call statement('create_schema') -%}
    create database if not exists {{ relation.without_identifier().include(database=False) }}
        {{ on_cluster_clause(relation)}}
        {{ adapter.clickhouse_db_engine_clause() }}
  {% endcall %}
{% endmacro %}

{% macro clickhouse__drop_schema(relation) -%}
  {%- call statement('drop_schema') -%}
    drop database if exists {{ relation.without_identifier().include(database=False) }} {{ on_cluster_clause(relation)}}
  {%- endcall -%}
{% endmacro %}

{% macro clickhouse__list_relations_without_caching(schema_relation) %}
  {#-
    Materialized views live in local ClickHouse databases. Listing every
    MaterializedView forces ClickHouse to open each MySQL/PostgreSQL database
    engine, so one unreachable remote database fails relation listing for every
    schema. Resolve the local database names first (system.databases does not
    connect) and pass them as literals, so the system.tables predicate drops
    those engines before any connection attempt. source() still reaches a
    remote schema: the outer query below filters by that schema directly.
  -#}
  {% set ns = namespace(local_databases=[]) %}
  {% if execute %}
    {% set local_db_rows = run_query("select name from system.databases where engine not in ('MySQL', 'PostgreSQL', 'MaterializedMySQL')") %}
    {% if local_db_rows is not none and local_db_rows.columns %}
      {% set ns.local_databases = local_db_rows.columns[0].values() %}
    {% endif %}
  {% endif %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    with mv_sources as (
      -- Find all MVs and their target tables (database and name of the MV, plus the SELECT SQL)
      select
        name as mv_name,
        database as mv_database,
        any(as_select) as mv_sql,
        {#- '\x3F' is the ClickHouse string-literal escape for '?' (regex quantifier); some drivers treat a literal '?' in query text as a bind parameter -#}
        any(replaceRegexpOne(create_table_query, '.*TO\\s+`\x3F([^`\\s(]+)`\x3F\\.`\x3F([^`\\s(]+)`\x3F.*', '\\1.\\2')) as target_fqn
      {% if adapter.get_clickhouse_cluster_name() -%}
      from clusterAllReplicas({{ adapter.get_clickhouse_cluster_name() }}, system.tables)
      {% else %}
      from system.tables
      {% endif %}
      where engine = 'MaterializedView'
        and create_table_query like '%TO %'
        {%- if ns.local_databases %}
        and database in ({% for db_name in ns.local_databases %}'{{ db_name | replace("'", "''") }}'{% if not loop.last %}, {% endif %}{% endfor %})
        {%- elif execute %}
        and 0
        {%- endif %}
      group by mv_name, mv_database
    )
    select
      t.name as name,
      t.database as schema,
      multiIf(
        t.engine = 'MaterializedView', 'materialized_view',
        t.engine = 'View', 'view',
        t.engine = 'Dictionary', 'dictionary',
        'table'
      ) as type,
      db.engine as db_engine,
      groupArrayIf(
        map('schema', mv_sources.mv_database, 'name', mv_sources.mv_name, 'sql', mv_sources.mv_sql),
        mv_sources.mv_name != ''
      ) as mvs_pointing_to_it,
      -- The refresh clause of a refreshable MV sits between the view name and the TO clause,
      -- e.g. CREATE MATERIALIZED VIEW db.mv REFRESH EVERY 2 MINUTE [APPEND] TO db.target ...
      max(position(substring(t.create_table_query, 1, position(t.create_table_query, ' TO ')), ' REFRESH ')) > 0 as is_refreshable,
      max(position(substring(t.create_table_query, 1, position(t.create_table_query, ' TO ')), ' APPEND')) > 0 as refreshable_append,
      {%- if adapter.get_clickhouse_cluster_name() -%}
        count(distinct _shard_num) > 1  as  is_on_cluster
        from clusterAllReplicas({{ adapter.get_clickhouse_cluster_name() }}, system.tables) as t
      {%- else -%}
        0 as is_on_cluster
        from system.tables as t
      {% endif %}
        join system.databases as db on t.database = db.name
        left join mv_sources on mv_sources.target_fqn = concat(t.database, '.', t.name)
      where schema = '{{ schema_relation.schema }}'
      group by name, schema, type, db_engine

  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}

{% macro clickhouse__get_columns_in_relation(relation) -%}
  {% call statement('get_columns', fetch_result=True) %}
    select name, type from system.columns where table = '{{ relation.identifier }}'
    {% if relation.schema %}
      {% if not relation.is_temporary %}
        and database = '{{ relation.schema }}'
      {% endif %}
    {% else %}
      {% do exceptions.warn("Relations should always come with a defined schema. Missing schema for " ~ relation.identifier) %}
    {% endif %}
    order by position
  {% endcall %}
  {{ return(sql_convert_columns_in_relation(load_result('get_columns').table)) }}
{% endmacro %}

{% macro clickhouse__drop_relation(relation, obj_type='table') -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    drop {{ obj_type }} if exists {{ relation }} {{ on_cluster_clause(relation, True)}}
  {%- endcall %}
{% endmacro %}

{% macro clickhouse__rename_relation(from_relation, to_relation, obj_type='table') -%}
  {% call statement('drop_relation') %}
    drop {{ obj_type }} if exists {{ to_relation }} {{ on_cluster_clause(to_relation)}}
  {% endcall %}
  {% call statement('rename_relation') %}
    rename {{ obj_type }} {{ from_relation }} to {{ to_relation }} {{ on_cluster_clause(from_relation)}}
  {% endcall %}
{% endmacro %}

{% macro clickhouse__truncate_relation(relation) -%}
  {% call statement('truncate_relation') -%}
    truncate table {{ relation }} {{ on_cluster_clause(relation)}}
  {%- endcall %}
{% endmacro %}

{% macro clickhouse__make_temp_relation(base_relation, suffix) %}
  {% set tmp_identifier = base_relation.identifier ~ suffix %}
  {% set tmp_relation = base_relation.incorporate(
                              path={"identifier": tmp_identifier, "schema": base_relation.schema}) -%}
  {% do return(tmp_relation) %}
{% endmacro %}


{% macro clickhouse__generate_database_name(custom_database_name=none, node=none) -%}
  {% do return('') %}
{%- endmacro %}

{% macro clickhouse__get_columns_in_query(select_sql) %}
  {{ return(adapter.get_column_schema_from_query(select_sql) | map(attribute='name') | list) }}
{% endmacro %}

{% macro clickhouse__alter_column_type(relation, column_name, new_column_type) -%}
  {% call statement('alter_column_type') %}
    alter table {{ relation }} {{ on_cluster_clause(relation)}} modify column {{ adapter.quote(column_name) }} {{ new_column_type }}
  {% endcall %}
{% endmacro %}

{% macro exchange_tables_atomic(old_relation, target_relation, obj_types='TABLES') %}

  {%- if adapter.get_clickhouse_cluster_name() is not none and obj_types == 'TABLES' and 'Replicated' in engine_clause() %}
    {%- call statement('exchange_table_sync_replica') -%}
      SYSTEM SYNC REPLICA  {{ on_cluster_clause(target_relation) }} {{ target_relation.schema }}.{{ target_relation.identifier }}
    {% endcall %}
  {%- endif %}
  {%- call statement('exchange_tables_atomic') -%}
    EXCHANGE {{ obj_types }} {{ old_relation }} AND {{ target_relation }} {{ on_cluster_clause(target_relation)}}
  {% endcall %}
{% endmacro %}
