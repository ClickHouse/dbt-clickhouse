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

  {# on_schema_change always have 'ignore' as default value, so we need a way to detect if the user has defined it.
   In unrendered_config you can find the defined settings even if these are defined at dbt_project level #}
  {% if 'on_schema_change' in config.model.unrendered_config %}
    {# user has defined one, getting it #}
    {%- set configured_on_schema_change = config.get('on_schema_change', 'ignore') -%}
  {% else %}
    {%- set configured_on_schema_change = none -%}
  {% endif %}

  {%- set repopulate_from_mvs_on_full_refresh = config.get('repopulate_from_mvs_on_full_refresh', False) -%}
  {%- set dbt_mvs_pointing_to_this_table = clickhouse__get_dbt_mvs_for_target(existing_relation) -%}

  {# If there is not existing relation, we can just create a new one #}
  {% if existing_relation is none %}
    {{ log('Creating new relation ' + target_relation.name )}}
    {% call statement('main') -%}
      {{ get_create_table_as_sql(False, target_relation, sql) }}
    {%- endcall %}

  {# If regular run was executed: Apply on_schema_change strategy #}
  {% elif not should_full_refresh() %}
    
    {# default to "fail" if this table is used by any dbt-managed MV. Added as a protection to prevent data loss -#}
    {%- if dbt_mvs_pointing_to_this_table and configured_on_schema_change is none -%}
      {{ log('Table ' ~ target_relation.name ~ ' is used as a target by a dbt-managed materialized view. Defaulting on_schema_change to "fail" to prevent data loss.', info=True) }}
      {%- set configured_on_schema_change = 'fail' -%}
    {%- endif -%}

    {# If definetively no on_schema_change is set, we behave as usual #}
    {% if configured_on_schema_change is none %}
      {% if existing_relation.can_exchange %}
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
    
    {# If on_schema_change is set, we apply the strategy #}
    {% else %}
      {%- set on_schema_change = incremental_validate_on_schema_change(configured_on_schema_change, default='ignore') -%}
      {% call statement('main') -%}
        Select 1
      {%- endcall %}
      {{ log('on_schema_change strategy for table: ' + on_schema_change) }}
      {%- if on_schema_change != 'ignore' -%}
        {%- set column_changes = adapter.check_incremental_schema_changes(on_schema_change, existing_relation, sql, materialization='table') -%}
        {% if column_changes %}
          {% do clickhouse__apply_column_changes(column_changes, existing_relation) %}
          {% set existing_relation = load_cached_relation(this) %}
        {% endif %}
      {%- endif %}
    {% endif %}
  
  {# Behaviour under full_refresh operations #}
  {% else %}
    {# Atomic full refresh with MV repopulation: when table is target of dbt MVs and repopulate_from_mvs_on_full_refresh is enabled #}
    {% if dbt_mvs_pointing_to_this_table and repopulate_from_mvs_on_full_refresh %}
      {{ log('Performing atomic full refresh with MV repopulation for ' ~ target_relation.name, info=True) }}

      {# Choose staging relation based on exchange support #}
      {% set staging_relation = backup_relation if existing_relation.can_exchange else intermediate_relation %}

      {# Create staging table with new schema (empty) #}
      {% call statement('main') -%}
        {{ get_create_table_as_sql(False, staging_relation, sql) }}
      {%- endcall %}

      {# Populate staging table from each MV's SELECT #}
      {% for mv_info in dbt_mvs_pointing_to_this_table %}
          {{ log('Repopulating from MV: ' ~ mv_info.name, info=True) }}
          {% set wrapped_sql = 'SELECT * FROM (' ~ mv_info.sql ~ ')' %}
          {% do run_query(clickhouse__insert_into(staging_relation, wrapped_sql, false, use_columns_from_sql=True)) %}
      {% endfor %}

      {# Swap tables #}
      {% if existing_relation.can_exchange %}
        {% do exchange_tables_atomic(backup_relation, existing_relation) %}
      {% else %}
        {{ adapter.rename_relation(existing_relation, backup_relation) }}
        {{ adapter.rename_relation(intermediate_relation, target_relation) }}
      {% endif %}

    {# Normal full refresh: no MV repopulation, just create a new, empty table #}
    {% else %}
      {% if existing_relation.can_exchange %}
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
    {% endif %}
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

{#-
  Get all dbt-managed materialized views that point to a target table.
  Returns a list of dictionaries with MV info including the SELECT SQL.
  Used by table materialization for atomic full refresh with MV repopulation.

  This uses the cached relation data (mvs_pointing_to_it) which already contains
  {schema, name, sql} dicts for each MV. We filter to only include MVs that
  are also defined in the dbt project (to exclude non-dbt MVs).

  Note: On first run, MVs don't exist in ClickHouse yet, so repopulation
  won't happen (which is correct - there's no data to preserve on first run).
-#}
{% macro clickhouse__get_dbt_mvs_for_target(relation) %}
  {%- set dbt_mvs = [] -%}
  {%- if relation is none or relation.mvs_pointing_to_it | length == 0 -%}
    {{ return(dbt_mvs) }}
  {%- endif -%}

  {%- set seen_mvs = [] -%}
  {%- for mv in relation.mvs_pointing_to_it -%}
    {%- set mv_key = mv.schema ~ '.' ~ mv.name -%}
    {%- if mv_key not in seen_mvs -%}
      {#- Only include MVs that are also defined in dbt (to filter out non-dbt MVs) -#}
      {%- for node in graph.nodes.values() -%}
        {%- if node.resource_type == 'model'
            and node.config.materialized == 'materialized_view'
            and node.schema == mv.schema
            and node.name == mv.name -%}
          {%- do dbt_mvs.append(mv) -%}
          {%- do seen_mvs.append(mv_key) -%}
        {%- endif -%}
      {%- endfor -%}
    {%- endif -%}
  {%- endfor -%}

  {{ return(dbt_mvs) }}
{% endmacro %}

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
         {{ add_index_and_projections(relation) }}

        {{ clickhouse__insert_into(relation, sql, has_contract) }}
    {%- endif %}
{%- endmacro %}

{#
    A macro that adds any configured projections or indexes at the same time.
    We optimise to reduce the number of ALTER TABLE statements that are run to avoid
    Code: 517.
    DB::Exception: Metadata on replica is not up to date with common metadata in Zookeeper.
    It means that this replica still not applied some of previous alters. Probably too many
    alters executing concurrently (highly not recommended).
#}
{% macro add_index_and_projections(relation) %}
    {%- set projections = config.get('projections', default=[]) -%}
    {%- set indexes = config.get('indexes', default=[]) -%}

    {% if projections | length > 0 or indexes | length > 0 %}
        {% call statement('add_projections_and_indexes') %}
            ALTER TABLE {{ relation }}
            {%- if projections %}
                {%- for projection in projections %}
                    ADD PROJECTION {{ projection.get('name') }} ({{ projection.get('query') }})
                    {%- if not loop.last or indexes | length > 0 -%}
                        ,
                    {% endif %}
                {%- endfor %}
            {%- endif %}
            {%- if indexes %}
                {%- for index in indexes %}
                    ADD INDEX {{ index.get('name') }} {{ index.get('definition') }}
                    {%- if not loop.last -%}
                        ,
                    {% endif %}
                {% endfor %}
            {% endif %}
        {% endcall %}
    {% endif %}
{% endmacro %}

{% macro create_table_or_empty(temporary, relation, sql, has_contract) -%}
    {%- set sql_header = config.get('sql_header', none) -%}

    {{ sql_header if sql_header is not none }}

    {% if temporary -%}
        create temporary table {{ relation.identifier }}
        engine Memory
        {{ adapter.get_model_settings(model, 'Memory') }}
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
        {{ adapter.get_model_settings(model, config.get('engine', default='MergeTree')) }}

        {%- if not has_contract %}
          {%- if not adapter.is_before_version('22.7.1.2484') %}
            empty
          {%- endif %}
          as (
            {{ sql }}
          )
        {%- endif %}
        {{ adapter.get_model_query_settings(model) }}
    {%- endif %}

{%- endmacro %}

{% macro clickhouse__insert_into(target_relation, sql, has_contract, use_columns_from_sql=False) %}
  {% if use_columns_from_sql %}
    {% set dest_columns = clickhouse__get_columns_in_query(sql) %}
    {%- set ns = namespace(quoted_cols=[]) -%}
    {%- for col in dest_columns -%}
      {%- set ns.quoted_cols = ns.quoted_cols + [adapter.quote(col)] -%}
    {%- endfor -%}
    {%- set dest_cols_csv = ns.quoted_cols | join(', ') -%}
  {%- else %}
    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
  {%- endif %}

  insert into {{ target_relation }}
        ({{ dest_cols_csv }})
  {%- if has_contract -%}
    -- Use a subquery to get columns in the right order
          SELECT {{ dest_cols_csv }}
          FROM (
            {{ sql }}
            )
  {%- else -%}
      {{ sql }}
  {%- endif -%}
  {{ adapter.get_model_query_settings(model) }}
{%- endmacro %}

{% macro codec_clause(codec_name) %}
  {%- if codec_name %}
      CODEC({{ codec_name }})
  {%- endif %}
{% endmacro %}
