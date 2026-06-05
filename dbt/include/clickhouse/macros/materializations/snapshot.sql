{% macro clickhouse__snapshot_hash_arguments(args) -%}
  halfMD5({%- for arg in args -%}
    coalesce(cast({{ arg }} as varchar ), '')
    {% if not loop.last %} || '|' || {% endif %}
  {%- endfor -%})
{%- endmacro %}

{% macro clickhouse__post_snapshot(staging_relation) %}
    {{ drop_relation_if_exists(staging_relation) }}
{% endmacro %}

{% macro build_snapshot_staging_table(strategy, sql, target_relation) %}
    {% set tmp_relation = make_temp_relation(target_relation) %}

    {% set select = snapshot_staging_table(strategy, sql, target_relation) %}

    {% call statement('build_snapshot_staging_relation') %}
        {{ create_table_as(False, tmp_relation, select) }}
    {% endcall %}

    {% do return(tmp_relation) %}
{% endmacro %}

{% macro clickhouse__snapshot_merge_sql(target, source, insert_cols) -%}
  {%- set insert_cols_csv = insert_cols | join(', ') -%}
  {%- set columns = config.get('snapshot_table_column_names') or get_snapshot_table_column_names() -%}
  {%- set valid_to_col = adapter.quote(columns.dbt_valid_to) -%}

  {%- set upsert = target.derivative('__snapshot_upsert') -%}
  {% call statement('create_upsert_relation') %}
    create table if not exists {{ upsert }} {{ on_cluster_clause(upsert) }} as {{ target }}
  {% endcall %}

  {% call statement('insert_unchanged_data') %}
    insert into {{ upsert }} ({{ insert_cols_csv }})
    select {% for column in insert_cols -%}
      {{ column }} {%- if not loop.last %}, {%- endif %}
    {%- endfor %}
    from {{ target }}
    where {{ columns.dbt_scd_id }} not in (
      select {{ source }}.{{ columns.dbt_scd_id }} from {{ source }}
    )
  {% endcall %}

 {% call statement('insert_updated_and_deleted') %}
    insert into {{ upsert }} ({{ insert_cols_csv }})
    with updates_and_deletes as (
      select
        {{ columns.dbt_scd_id }},
        {{ columns.dbt_valid_to }}
      from {{ source }}
      where dbt_change_type IN ('update', 'delete')
    )
    select {% for column in insert_cols %}
      {%- if column == valid_to_col -%}
        updates_and_deletes.{{ columns.dbt_valid_to }} as {{ column }}
      {%- else -%}
        target.{{ column }} as {{ column }}
      {%- endif %} {%- if not loop.last %}, {%- endif %}
    {%- endfor %}
    from {{ target }} target
    join updates_and_deletes on target.{{ columns.dbt_scd_id }} = updates_and_deletes.{{ columns.dbt_scd_id }};
  {% endcall %}

  {% call statement('insert_new') %}
    insert into {{ upsert }} ({{ insert_cols_csv }})
    select {% for column in insert_cols -%}
      {{ column }} {%- if not loop.last %}, {%- endif %}
    {%- endfor %}
    from {{ source }}
    where {{ source }}.dbt_change_type IN ('insert');
  {% endcall %}

  {% if target.can_exchange %}
    {% do exchange_tables_atomic(upsert, target) %}
    {% call statement('drop_exchanged_relation') %}
      drop table if exists {{ upsert }} {{ on_cluster_clause(upsert) }};
    {% endcall %}
  {% else %}
    {% call statement('drop_target_relation') %}
      drop table if exists {{ target }} {{ on_cluster_clause(target) }};
    {% endcall %}
    {% call statement('rename_upsert_relation') %}
      rename table {{ upsert }} to {{ target }};
    {% endcall %}
  {% endif %}

  {% do return ('select 1') %}
{% endmacro %}


{#
  Rebased on dbt-core `default__snapshot_staging_table` -- keep in sync with upstream.
  ClickHouse-only deltas (the body is otherwise verbatim dbt-core):
    1. trailing `settings join_use_nulls = 1` -- else LEFT JOIN fills unmatched rows with
       type defaults, not NULL, so the `... is null` anti-joins (inserts/hard deletes) never fire.
    2. check strategy: `strategy.updated_at` is `now()`, re-evaluated inconsistently across
       this multi-CTE query, so `nullif(now(), now())` may be non-NULL and wrongly close the
       current row. Pin it in a single-row `snapshot_time` CTE, cross-joined so `ts` is one
       materialized value; repoint `strategy.updated_at` at it; strip the leaked `ts` with
       `* EXCEPT (ts)`. (A `(select now())` scalar subquery is NOT reliable -- it gets re-evaluated.)
    3. merge is insert + EXCHANGE TABLES in `clickhouse__snapshot_merge_sql` (ClickHouse has no `MERGE`).
#}
{% macro clickhouse__snapshot_staging_table(strategy, source_sql, target_relation) -%}
    {% set columns = config.get('snapshot_table_column_names') or get_snapshot_table_column_names() %}
    {% if strategy.hard_deletes == 'new_record' %}
        {% set new_scd_id = snapshot_hash_arguments([columns.dbt_scd_id, snapshot_get_time()]) %}
    {% endif %}
    {#-
        Pin now() for the check strategy (see header). Detect it via `snapshot_get_time()`
        rather than the literal `now()` (survives changes to `clickhouse__current_timestamp()`;
        skips real per-row `updated_at` columns). Rebind a *local copy* of `strategy`, the
        materialization calls this macro twice with the same object.
    -#}
    {% set check_strategy = (strategy.updated_at | trim) == (snapshot_get_time() | trim) %}
    {% set time_join = ', snapshot_time' if check_strategy else '' %}
    {% set final_star = '* EXCEPT (ts)' if check_strategy else '*' %}
    {%- if check_strategy %}
        {% set snapshot_time_expr = strategy.updated_at %}
        {% set strategy = {
            'unique_key': strategy.unique_key,
            'updated_at': 'snapshot_time.ts',
            'row_changed': strategy.row_changed,
            'scd_id': strategy.scd_id,
            'invalidate_hard_deletes': strategy.invalidate_hard_deletes,
            'hard_deletes': strategy.hard_deletes,
        } %}
    {%- endif %}
    with
    {%- if check_strategy %}
    snapshot_time as (
        select {{ snapshot_time_expr }} as ts
    ),
    {%- endif %}
    snapshot_query as (

        {{ source_sql }}

    ),

    snapshotted_data as (

        select *, {{ unique_key_fields(strategy.unique_key) }}
        from {{ target_relation }}
        where
            {% if config.get('dbt_valid_to_current') %}
                {% set source_unique_key = columns.dbt_valid_to | trim %}
                {% set target_unique_key = config.get('dbt_valid_to_current') | trim %}
                ( {{ equals(source_unique_key, target_unique_key) }} or {{ source_unique_key }} is null )
            {% else %}
                {{ columns.dbt_valid_to }} is null
            {% endif %}

    ),

    insertions_source_data as (

        select *, {{ unique_key_fields(strategy.unique_key) }},
            {{ strategy.updated_at }} as {{ columns.dbt_updated_at }},
            {{ strategy.updated_at }} as {{ columns.dbt_valid_from }},
            {{ get_dbt_valid_to_current(strategy, columns) }},
            {{ strategy.scd_id }} as {{ columns.dbt_scd_id }}

        from snapshot_query{{ time_join }}
    ),

    updates_source_data as (

        select *, {{ unique_key_fields(strategy.unique_key) }},
            {{ strategy.updated_at }} as {{ columns.dbt_updated_at }},
            {{ strategy.updated_at }} as {{ columns.dbt_valid_from }},
            {{ strategy.updated_at }} as {{ columns.dbt_valid_to }}

        from snapshot_query{{ time_join }}
    ),

    {%- if strategy.hard_deletes == 'invalidate' or strategy.hard_deletes == 'new_record' %}

    deletes_source_data as (

        select *, {{ unique_key_fields(strategy.unique_key) }}
        from snapshot_query{{ time_join }}
    ),
    {% endif %}

    insertions as (

        select
            'insert' as dbt_change_type,
            source_data.*
          {%- if strategy.hard_deletes == 'new_record' -%}
            ,'False' as {{ columns.dbt_is_deleted }}
          {%- endif %}

        from insertions_source_data as source_data
        left outer join snapshotted_data
            on {{ unique_key_join_on(strategy.unique_key, "snapshotted_data", "source_data") }}
            where {{ unique_key_is_null(strategy.unique_key, "snapshotted_data") }}
            or ({{ unique_key_is_not_null(strategy.unique_key, "snapshotted_data") }} and (
               {{ strategy.row_changed }} {%- if strategy.hard_deletes == 'new_record' -%} or snapshotted_data.{{ columns.dbt_is_deleted }} = 'True' {% endif %}
            )

        )

    ),

    updates as (

        select
            'update' as dbt_change_type,
            source_data.*,
            snapshotted_data.{{ columns.dbt_scd_id }}
          {%- if strategy.hard_deletes == 'new_record' -%}
            , snapshotted_data.{{ columns.dbt_is_deleted }}
          {%- endif %}

        from updates_source_data as source_data
        join snapshotted_data
            on {{ unique_key_join_on(strategy.unique_key, "snapshotted_data", "source_data") }}
        where (
            {{ strategy.row_changed }}  {%- if strategy.hard_deletes == 'new_record' -%} or snapshotted_data.{{ columns.dbt_is_deleted }} = 'True' {% endif %}
        )
    )

    {%- if strategy.hard_deletes == 'invalidate' or strategy.hard_deletes == 'new_record' %}
    ,
    deletes as (

        select
            'delete' as dbt_change_type,
            source_data.*,
            {{ snapshot_get_time() }} as {{ columns.dbt_valid_from }},
            {{ snapshot_get_time() }} as {{ columns.dbt_updated_at }},
            {{ snapshot_get_time() }} as {{ columns.dbt_valid_to }},
            snapshotted_data.{{ columns.dbt_scd_id }}
          {%- if strategy.hard_deletes == 'new_record' -%}
            , snapshotted_data.{{ columns.dbt_is_deleted }}
          {%- endif %}
        from snapshotted_data
        left join deletes_source_data as source_data
            on {{ unique_key_join_on(strategy.unique_key, "snapshotted_data", "source_data") }}
            where {{ unique_key_is_null(strategy.unique_key, "source_data") }}

            {%- if strategy.hard_deletes == 'new_record' %}
            and not (
                --avoid updating the record's valid_to if the latest entry is marked as deleted
                snapshotted_data.{{ columns.dbt_is_deleted }} = 'True'
                and
                {% if config.get('dbt_valid_to_current') -%}
                    snapshotted_data.{{ columns.dbt_valid_to }} = {{ config.get('dbt_valid_to_current') }}
                {%- else -%}
                    snapshotted_data.{{ columns.dbt_valid_to }} is null
                {%- endif %}
            )
            {%- endif %}
    )
    {%- endif %}

    {%- if strategy.hard_deletes == 'new_record' %}
        {% set snapshotted_cols = get_list_of_column_names(get_columns_in_relation(target_relation)) %}
        {% set source_sql_cols = get_column_schema_from_query(source_sql) %}
    ,
    deletion_records as (

        select
            'insert' as dbt_change_type,
            {#/*
                If a column has been added to the source it won't yet exist in the
                snapshotted table so we insert a null value as a placeholder for the column.
             */#}
            {%- for col in source_sql_cols -%}
            {%- if col.name in snapshotted_cols -%}
            snapshotted_data.{{ adapter.quote(col.column) }},
            {%- else -%}
            NULL as {{ adapter.quote(col.column) }},
            {%- endif -%}
            {% endfor -%}
            {%- if strategy.unique_key | is_list -%}
                {%- for key in strategy.unique_key -%}
            snapshotted_data.{{ key }} as dbt_unique_key_{{ loop.index }},
                {% endfor -%}
            {%- else -%}
            snapshotted_data.dbt_unique_key as dbt_unique_key,
            {% endif -%}
            {{ snapshot_get_time() }} as {{ columns.dbt_valid_from }},
            {{ snapshot_get_time() }} as {{ columns.dbt_updated_at }},
            snapshotted_data.{{ columns.dbt_valid_to }} as {{ columns.dbt_valid_to }},
            {{ new_scd_id }} as {{ columns.dbt_scd_id }},
            'True' as {{ columns.dbt_is_deleted }}
            {#- carry a `ts` column so `* EXCEPT (ts)` is uniform across all union branches -#}
            {%- if check_strategy %}, {{ snapshot_get_time() }} as ts{%- endif %}
        from snapshotted_data
        left join deletes_source_data as source_data
            on {{ unique_key_join_on(strategy.unique_key, "snapshotted_data", "source_data") }}
        where {{ unique_key_is_null(strategy.unique_key, "source_data") }}
        and not (
            --avoid inserting a new record if the latest one is marked as deleted
            snapshotted_data.{{ columns.dbt_is_deleted }} = 'True'
            and
            {% if config.get('dbt_valid_to_current') -%}
                snapshotted_data.{{ columns.dbt_valid_to }} = {{ config.get('dbt_valid_to_current') }}
            {%- else -%}
                snapshotted_data.{{ columns.dbt_valid_to }} is null
            {%- endif %}
            )

    )
    {%- endif %}

    select {{ final_star }} from insertions
    union all
    select {{ final_star }} from updates
    {%- if strategy.hard_deletes == 'invalidate' or strategy.hard_deletes == 'new_record' %}
    union all
    select {{ final_star }} from deletes
    {%- endif %}
    {%- if strategy.hard_deletes == 'new_record' %}
    union all
    select {{ final_star }} from deletion_records
    {%- endif %}
    -- join_use_nulls is required so that the unmatched side of the LEFT JOINs above
    -- yields NULL (and not ClickHouse type-default values), otherwise the
    -- `... is null` anti-join checks for inserts/hard deletes never match.
    settings join_use_nulls = 1

{%- endmacro %}