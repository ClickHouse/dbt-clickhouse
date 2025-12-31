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

  {# Note for the future: checking if the MV is defined by dbt is just for backwards compatibility. It looks unlikely that
  a user would want to recreate the table if ANY mv is pointing to it, even if it's not defined by dbt. #}
  {%- set is_table_used_by_dbt_mv = clickhouse__table_is_target_of_dbt_mv(existing_relation) -%}

  {# on_schema_change always have 'ignore' as default value, so we need a way to detect if the user has defined it.
   In unrendered_config you can find the defined settings even if these are defined at dbt_project level #}
  {% if 'on_schema_change' in config.model.unrendered_config %}
    {# user has defined one, getting it #}
    {%- set configured_on_schema_change = config.get('on_schema_change', 'ignore') -%}
  {% else %}
    {%- set configured_on_schema_change = none -%}
  {% endif %}

  {% if backup_relation is none %}
    {{ log('Creating new relation ' + target_relation.name )}}
    -- There is not existing relation, so we can just create
    {% call statement('main') -%}
      {{ get_create_table_as_sql(False, target_relation, sql) }}
    {%- endcall %}

  -- if this table is used by any dbt-managed MV we need to protect it and not recreate it on a `dbt run`. on_schema_change strategies will be applied to iterate it.
  {% elif should_full_refresh() or (configured_on_schema_change is none and not is_table_used_by_dbt_mv) %}
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
  {% else %}
  
    {#- Apply on_schema_change strategy. if this table is used by any dbt-managed MV we need to protect it from schema changes so a default "fail" is applied to prevent data loss -#}
    {%- if is_table_used_by_dbt_mv and configured_on_schema_change is none -%}
      {{ log('Table ' ~ target_relation.name ~ ' is used as a target by a dbt-managed materialized view. Defaulting on_schema_change to "fail" to prevent data loss.', info=True) }}
      {%- set configured_on_schema_change = 'fail' -%}
    {%- endif -%}

    {%- set on_schema_change = incremental_validate_on_schema_change(configured_on_schema_change, default='ignore') -%}
    {% call statement('main') -%}
      Select 1
    {%- endcall %}
    {{ log('on_schema_change strategy for table: ' + on_schema_change) }}
    {%- if on_schema_change != 'ignore' -%}
      {%- set column_changes = adapter.check_incremental_schema_changes(on_schema_change, existing_relation, sql) -%}
      {% if column_changes %}
        {% do clickhouse__apply_column_changes(column_changes, existing_relation) %}
        {% set existing_relation = load_cached_relation(this) %}
      {% endif %}
    {%- endif %}

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

{% macro clickhouse__table_is_target_of_dbt_mv(relation) %}
  {%- if relation.mvs_pointing_to_it | length == 0 -%}
    {{ return(false) }}
  {%- endif -%}

  {%- for mv_fqn in relation.mvs_pointing_to_it -%}
    {%- for node in graph.nodes.values() -%}
      {%- if node.resource_type == 'model' 
          and node.config.materialized == 'materialized_view'
          and node.schema == mv_fqn.split('.')[0]
          and node.name == mv_fqn.split('.')[1] -%}
        {{ return(True) }}
      {%- endif -%}
    {%- endfor -%}
  {%- endfor -%}
  {{ return(False) }}
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

{% macro clickhouse__insert_into(target_relation, sql, has_contract) %}
  {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
  {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}

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
