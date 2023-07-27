{% materialization incremental, adapter='clickhouse' %}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}

  {%- set unique_key = config.get('unique_key') -%}
  {% if unique_key is not none and unique_key|length == 0 %}
    {% set unique_key = none %}
  {% endif %}
  {% if unique_key is iterable and (unique_key is not string and unique_key is not mapping) %}
     {% set unique_key = unique_key|join(', ') %}
  {% endif %}
  {%- set inserts_only = config.get('inserts_only') -%}
  {%- set grant_config = config.get('grants') -%}
  {%- set full_refresh_mode = (should_full_refresh() or existing_relation.is_view) -%}
  {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') -%}

  {%- set intermediate_relation = make_intermediate_relation(target_relation)-%}
  {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation)-%}
  {%- set preexisting_backup_relation = load_cached_relation(backup_relation) -%}

  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}
  {% set to_drop = [] %}

  {% if existing_relation is none %}
    -- No existing table, simply create a new one
    {% call statement('main') %}
        {{ get_create_table_as_sql(False, target_relation, sql) }}
    {% endcall %}

  {% elif full_refresh_mode %}
    -- Completely replacing the old table, so create a temporary table and then swap it
    {% call statement('main') %}
        {{ get_create_table_as_sql(False, intermediate_relation, sql) }}
    {% endcall %}
    {% set need_swap = true %}

  {% elif inserts_only or unique_key is none -%}
    -- There are no updates/deletes or duplicate keys are allowed.  Simply add all of the new rows to the existing
    -- table. It is the user's responsibility to avoid duplicates.  Note that "inserts_only" is a ClickHouse adapter
    -- specific configurable that is used to avoid creating an expensive intermediate table.
    {% call statement('main') %}
        {{ clickhouse__insert_into(target_relation, sql) }}
    {% endcall %}

  {% else %}
    {% set schema_changes = none %}
    {% set incremental_strategy = adapter.calculate_incremental_strategy(config.get('incremental_strategy'))  %}
    {% set incremental_predicates = config.get('predicates', none) or config.get('incremental_predicates', none) %}
    {% if on_schema_change != 'ignore' %}
      {%- set schema_changes = check_for_schema_changes(existing_relation, target_relation) -%}
      {% if schema_changes['schema_changed'] and incremental_strategy in ('append', 'delete_insert') %}
        {% set incremental_strategy = 'legacy' %}
        {% do log('Schema changes detected, switching to legacy incremental strategy') %}
      {% endif %}
    {% endif %}
    {% if incremental_strategy != 'delete_insert' and incremental_predicates %}
      {% do exceptions.raise_compiler_error('Cannot apply incremental predicates with ' + incremental_strategy + ' strategy.') %}
    {% endif %}
    {% if incremental_strategy == 'legacy' %}
      {% do clickhouse__incremental_legacy(existing_relation, intermediate_relation, schema_changes, unique_key) %}
      {% set need_swap = true %}
    {% elif incremental_strategy == 'delete_insert' %}
      {% do clickhouse__incremental_delete_insert(existing_relation, unique_key, incremental_predicates) %}
    {% elif incremental_strategy == 'append' %}
      {% call statement('main') %}
        {{ clickhouse__insert_into(target_relation, sql) }}
      {% endcall %}
    {% endif %}
  {% endif %}

  {% if need_swap %}
      {% if existing_relation.can_exchange %}
        {% do adapter.rename_relation(intermediate_relation, backup_relation) %}
        {% do exchange_tables_atomic(backup_relation, target_relation) %}
      {% else %}
        {% do adapter.rename_relation(target_relation, backup_relation) %}
        {% do adapter.rename_relation(intermediate_relation, target_relation) %}
      {% endif %}
      {% do to_drop.append(backup_relation) %}
  {% endif %}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {% if existing_relation is none or existing_relation.is_view or should_full_refresh() %}
    {% do create_indexes(target_relation) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% do adapter.commit() %}

  {% for rel in to_drop %}
      {% do adapter.drop_relation(rel) %}
  {% endfor %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}


{% macro process_schema_changes(on_schema_change, source_relation, target_relation) %}

    {%- set schema_changes_dict = check_for_schema_changes(source_relation, target_relation) -%}
    {% if not schema_changes_dict['schema_changed'] %}
      {{ return }}
    {% endif %}

    {% if on_schema_change == 'fail' %}
      {% set fail_msg %}
          The source and target schemas on this incremental model are out of sync!
          They can be reconciled in several ways:
            - set the `on_schema_change` config to either append_new_columns or sync_all_columns, depending on your situation.
            - Re-run the incremental model with `full_refresh: True` to update the target schema.
            - update the schema manually and re-run the process.
      {% endset %}
      {% do exceptions.raise_compiler_error(fail_msg) %}
      {{ return }}
    {% endif %}

    {% do sync_column_schemas(on_schema_change, target_relation, schema_changes_dict) %}

{% endmacro %}


{% macro clickhouse__incremental_legacy(existing_relation, intermediate_relation, on_schema_change, unique_key, is_distributed=False) %}
    {% set new_data_relation = existing_relation.incorporate(path={"identifier": model['name'] + '__dbt_new_data'}) %}
    {{ drop_relation_if_exists(new_data_relation) }}
    {%- set distributed_new_data_relation = existing_relation.incorporate(path={"identifier": model['name'] + '__dbt_distributed_new_data'}) -%}

    {%- set inserted_relation = intermediate_relation -%}
    {%- set inserting_relation = new_data_relation -%}

    -- First create a temporary table for all of the new data
    {% if is_distributed %}
      -- Need to use distributed table to have data on all shards
      {%- set inserting_relation = distributed_new_data_relation -%}
      {{ create_distributed_local_table(distributed_new_data_relation, new_data_relation, existing_relation, sql) }}
    {% else %}
      {% call statement('create_new_data_temp') %}
        {{ get_create_table_as_sql(False, new_data_relation, sql) }}
      {% endcall %}
    {% endif %}

    -- Next create another temporary table that will eventually be used to replace the existing table.  We can't
    -- use the table just created in the previous step because we don't want to override any updated rows with
    -- old rows when we insert the old data
    {% if is_distributed %}
      {%- set distributed_intermediate_relation = make_intermediate_relation(existing_relation) -%}
      {%- set inserted_relation = distributed_intermediate_relation -%}
      {{ create_distributed_local_table(distributed_intermediate_relation, intermediate_relation, existing_relation) }}
    {% else %}
      {% call statement('main') %}
          create table {{ intermediate_relation }} as {{ new_data_relation }} {{ on_cluster_clause() }}
      {% endcall %}
    {% endif %}

    -- Insert all the existing rows into the new temporary table, ignoring any rows that have keys in the "new data"
    -- table.
    {%- set dest_columns = adapter.get_columns_in_relation(existing_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
    {% call statement('insert_existing_data') %}
        insert into {{ inserted_relation }} ({{ dest_cols_csv }})
        select {{ dest_cols_csv }}
        from {{ existing_relation }}
          where ({{ unique_key }}) not in (
            select {{ unique_key }}
            from {{ inserting_relation }}
          )
       {{ adapter.get_model_settings(model) }}
    {% endcall %}

    -- Insert all of the new data into the temporary table
    {% call statement('insert_new_data') %}
     insert into {{ inserted_relation }} ({{ dest_cols_csv }})
        select {{ dest_cols_csv }}
        from {{ inserting_relation }}
      {{ adapter.get_model_settings(model) }}
    {% endcall %}

    {% do adapter.drop_relation(new_data_relation) %}
    {{ drop_relation_if_exists(distributed_new_data_relation) }}
    {{ drop_relation_if_exists(distributed_intermediate_relation) }}

{% endmacro %}


{% macro clickhouse__incremental_delete_insert(existing_relation, unique_key, incremental_predicates, is_distributed=False) %}
    {% set new_data_relation = existing_relation.incorporate(path={"identifier": model['name']
       + '__dbt_new_data_' + invocation_id.replace('-', '_')}) %}
    {{ drop_relation_if_exists(new_data_relation) }}
    {%- set distributed_new_data_relation = existing_relation.incorporate(path={"identifier": model['name'] + '__dbt_distributed_new_data'}) -%}

    {%- set inserting_relation = new_data_relation -%}

    {% if is_distributed %}
      -- Need to use distributed table to have data on all shards
      {%- set inserting_relation = distributed_new_data_relation -%}
      {{ create_distributed_local_table(distributed_new_data_relation, new_data_relation, existing_relation, sql) }}
    {% else %}
      {% call statement('main') %}
        {{ get_create_table_as_sql(False, new_data_relation, sql) }}
      {% endcall %}
    {% endif %}

    {% call statement('delete_existing_data') %}
      delete from {{ existing_relation }} where ({{ unique_key }}) in (select {{ unique_key }}
                                          from {{ inserting_relation }})
      {%- if incremental_predicates %}
        {% for predicate in incremental_predicates %}
            and {{ predicate }}
        {% endfor %}
      {%- endif -%};
    {% endcall %}

    {%- set dest_columns = adapter.get_columns_in_relation(existing_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
    {% call statement('insert_new_data') %}
        insert into {{ existing_relation }} select {{ dest_cols_csv }} from {{ inserting_relation }}
    {% endcall %}
    {% do adapter.drop_relation(new_data_relation) %}
    {{ drop_relation_if_exists(distributed_new_data_relation) }}
{% endmacro %}
