{% macro clickhouse__snapshot_hash_arguments(args) -%}
  halfMD5({%- for arg in args -%}
    coalesce(cast({{ arg }} as varchar ), '')
    {% if not loop.last %} || '|' || {% endif %}
  {%- endfor -%})
{%- endmacro %}

{% macro clickhouse__snapshot_string_as_time(timestamp) -%}
  {%- set result = "toDateTime('" ~ timestamp ~ "')" -%}
  {{ return(result) }}
{%- endmacro %}

{% materialization snapshot, adapter='clickhouse' %}
  {%- set config = model['config'] -%}

  {%- set target_table = model.get('alias', model.get('name')) -%}

  {%- set strategy_name = config.get('strategy') -%}
  {%- set unique_key = config.get('unique_key') %}

  {% if not adapter.check_schema_exists(model.database, model.schema) %}
    {% do create_schema(model.database, model.schema) %}
  {% endif %}

  {% set target_relation_exists, target_relation = get_or_create_relation(
          database=none,
          schema=model.schema,
          identifier=target_table,
          type='table') -%}

  {%- if not target_relation.is_table -%}
    {% do exceptions.relation_wrong_type(target_relation, 'table') %}
  {%- endif -%}


  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set strategy_macro = strategy_dispatch(strategy_name) %}
  {% set strategy = strategy_macro(model, "snapshotted_data", "source_data", config, target_relation_exists) %}

  {% if not target_relation_exists %}

    {% set build_sql = build_snapshot_table(strategy, model['compiled_sql']) %}
    {% set final_sql = create_table_as(False, target_relation, build_sql) %}

    {% call statement('main') %}
        {{ final_sql }}
    {% endcall %}

  {% else %}

    {{ adapter.valid_snapshot_target(target_relation) }}

    {% set staging_table = build_snapshot_staging_table(strategy, sql, target_relation) %}

    {% do adapter.expand_target_column_types(from_relation=staging_table,
                                             to_relation=target_relation) %}

    {% set missing_columns = adapter.get_missing_columns(staging_table, target_relation)
                                   | rejectattr('name', 'equalto', 'dbt_change_type')
                                   | rejectattr('name', 'equalto', 'DBT_CHANGE_TYPE')
                                   | rejectattr('name', 'equalto', 'dbt_unique_key')
                                   | rejectattr('name', 'equalto', 'DBT_UNIQUE_KEY')
                                   | list %}

    {% do create_columns(target_relation, missing_columns) %}

    {% set source_columns = adapter.get_columns_in_relation(staging_table)
                                 | rejectattr('name', 'equalto', 'dbt_change_type')
                                 | rejectattr('name', 'equalto', 'DBT_CHANGE_TYPE')
                                 | rejectattr('name', 'equalto', 'dbt_unique_key')
                                 | rejectattr('name', 'equalto', 'DBT_UNIQUE_KEY')
                                 | list %}

    {%- set quoted_source_columns = get_columns_in_query('select * from ' ~ target_relation) -%}

    {% set upsert_relation = target_relation ~ '__snapshot_upsert' %}

    {% do clickhouse__snapshot_merge_sql_one(
          target = target_relation,
          source = staging_table,
          insert_cols = quoted_source_columns,
          upsert = upsert_relation) 
    %}

    {% call statement('main') %}
        select 1
    {% endcall %}

  {% endif %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ adapter.commit() }}

  {% if staging_table is defined %}
    {% do post_snapshot(staging_table) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}

{% macro snapshot_staging_table(strategy, source_sql, target_relation) -%}
    select
      'insert' as dbt_change_type,
      source_data.*
    from (
      select
        *,
        {{ strategy.unique_key }} as dbt_unique_key,
        {{ strategy.updated_at }} as dbt_updated_at,
        {{ strategy.updated_at }} as dbt_valid_from,
        nullif({{ strategy.updated_at }}, {{ strategy.updated_at }}) as dbt_valid_to,
        {{ strategy.scd_id }} as dbt_scd_id
      from (
        {{ source_sql }}
      ) as snapshot_query
    ) as source_data
    left outer join (
      select *,
        {{ strategy.unique_key }} as dbt_unique_key
      from {{ target_relation }}
    ) as snapshotted_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
    where snapshotted_data.dbt_unique_key is null
      or (snapshotted_data.dbt_unique_key is not null
      and snapshotted_data.dbt_valid_to is null
      and (
        {{ strategy.row_changed }}
      )
    )

    union all

    select
      'update' as dbt_change_type,
      source_data.*,
      snapshotted_data.dbt_scd_id
    from (
      select
        *,
        {{ strategy.unique_key }} as dbt_unique_key,
        {{ strategy.updated_at }} as dbt_updated_at,
        {{ strategy.updated_at }} as dbt_valid_from,
        {{ strategy.updated_at }} as dbt_valid_to
      from (
        {{ source_sql }}
      ) as snapshot_query
    ) as source_data
    join (
      select *,
        {{ strategy.unique_key }} as dbt_unique_key
      from {{ target_relation }}
    ) as snapshotted_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
    where snapshotted_data.dbt_valid_to is null
    and (
      {{ strategy.row_changed }}
    )
{%- endmacro %}

{% macro clickhouse__snapshot_merge_sql_one(target, source, insert_cols, upsert) -%}
  {%- set insert_cols_csv = insert_cols | join(', ') -%}

  {% call statement('create_upsert_relation') %}
    create table if not exists {{ upsert }} as {{ target }};
  {% endcall %}

  {% call statement('insert_unchanged_date') %}
    insert into {{ upsert }} ({{ insert_cols_csv }})
    select {% for column in insert_cols -%}
      {{ column }} {%- if not loop.last %}, {%- endif %}
    {%- endfor %}
    from {{ target }}
    where dbt_scd_id not in (
      select {{ source }}.dbt_scd_id from {{ source }} 
    )
  {% endcall %}

  {% call statement('insert_changed_date') %}
    insert into {{ upsert }} ({{ insert_cols_csv }})
    select {% for column in insert_cols -%}
      {{ column }} {%- if not loop.last %}, {%- endif %}
    {%- endfor %}
    from {{ source }}
    where {{ source }}.dbt_change_type IN ('insert', 'update', 'delete');
  {% endcall %}

  {% call statement('drop_target_relation') %}
    drop table if exists {{ target }};
  {% endcall %}

  {% call statement('rename_upsert_relation') %}
    rename table {{ upsert }} to {{ target }};
  {% endcall %}
{% endmacro %}
