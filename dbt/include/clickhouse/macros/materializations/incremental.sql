{% materialization incremental, adapter='clickhouse' -%}

  {% set unique_key = config.get('unique_key') %}
  {% set inserts_only = config.get('inserts_only') %}

  {% set target_relation = this.incorporate(type='table') %}
  {% set existing_relation = load_relation(this) %}
  {% set tmp_relation = make_temp_relation(target_relation) %}
  {%- set full_refresh_mode = (should_full_refresh()) -%}

  {% set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') %}

  {% set tmp_identifier = model['name'] + '__dbt_tmp' %}
  {% set old_identifier = model['name'] + '__dbt_old' %}
  {% set backup_identifier = model['name'] + "__dbt_backup" %}

  -- the intermediate_ and backup_ relations should not already exist in the database; get_relation
  -- will return None in that case. Otherwise, we get a relation that we can drop
  -- later, before we try to use this name for the current operation. This has to happen before
  -- BEGIN, in a separate transaction
  {% set preexisting_intermediate_relation = adapter.get_relation(identifier=tmp_identifier, 
                                                                  schema=schema,
                                                                  database=database) %}                                               
  {% set preexisting_backup_relation = adapter.get_relation(identifier=backup_identifier,
                                                            schema=schema,
                                                            database=database) %}
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set to_drop = [] %}

  {# -- first check whether we want to full refresh for source view or config reasons #}
  {% set trigger_full_refresh = (full_refresh_mode or existing_relation.is_view) %}

  {% if existing_relation is none %}
      {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% elif trigger_full_refresh %}
      {#-- Make sure the backup doesn't exist so we don't encounter issues with the rename below #}
      {% set tmp_identifier = model['name'] + '__dbt_tmp' %}
      {% set backup_identifier = model['name'] + '__dbt_backup' %}
      {% set intermediate_relation = existing_relation.incorporate(path={"identifier": tmp_identifier}) %}
      {% set backup_relation = existing_relation.incorporate(path={"identifier": backup_identifier}) %}

      {% set build_sql = create_table_as(False, intermediate_relation, sql) %}
      {% set need_swap = true %}
      {% do to_drop.append(backup_relation) %}
  {% else %}
    {%- if inserts_only or unique_key is none -%}
        -- Run incremental insert without updates - updated rows will be added too to the table and will create
        -- duplicate entries in the table. It is the user's responsibility to avoid updates.
        {% set build_sql = clickhouse__incremental_insert(target_relation, sql) %}
    {% else %}
        {% set old_relation = existing_relation.incorporate(path={"identifier": old_identifier}) %}
        -- Create a table with only updated rows.
        {% do run_query(create_table_as(False, tmp_relation, sql)) %}
        {% if on_schema_change != 'ignore' %}
            -- Update schema types if necessary.
            {% do adapter.expand_target_column_types(
                 from_relation=tmp_relation,
                 to_relation=target_relation) %}
            {% do process_schema_changes(on_schema_change, tmp_relation, existing_relation) %}
        {% endif %}

        {% do adapter.rename_relation(target_relation, old_relation) %}
        -- Create a new target table.
        {% set create_sql = clickhouse__incremental_create(old_relation, target_relation) %}
        {% call statement('main') %}
            {{ create_sql }}
        {% endcall %}
        -- Insert all untouched rows to the target table.
        {% set currect_insert_sql = clickhouse__incremental_cur_insert(old_relation, tmp_relation, target_relation, unique_key=unique_key) %}
        {% call statement('main') %}
            {{ currect_insert_sql }}
        {% endcall %}
        -- Insert all incremental updates to the target table.
        {% set build_sql = clickhouse__incremental_insert_from_table(tmp_relation, target_relation) %}
        {% do to_drop.append(old_relation) %}
        {% do to_drop.append(tmp_relation) %}
    {% endif %}
  {% endif %}

  {% call statement('main') %}
      {{ build_sql }}
  {% endcall %}

  {% if need_swap %} 
      {% do adapter.rename_relation(target_relation, backup_relation) %} 
      {% do adapter.rename_relation(intermediate_relation, target_relation) %} 
  {% endif %}

  {% do persist_docs(target_relation, model) %}

  {% if existing_relation is none or existing_relation.is_view or should_full_refresh() %}
    {% do create_indexes(target_relation) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {% do adapter.commit() %}

  {% for rel in to_drop %}
      {% do adapter.drop_relation(rel) %}
  {% endfor %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}

{% macro clickhouse__incremental_create(old_relation, target_relation) %}
  create table {{ target_relation }} as {{ old_relation }}
{%- endmacro %}

{% macro clickhouse__incremental_cur_insert(old_relation, tmp_relation, target_relation, unique_key=none) %}
  {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
  {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}

  insert into {{ target_relation }} ({{ dest_cols_csv }})
  select {{ dest_cols_csv }}
  from {{ old_relation }}
  where ({{ unique_key }}) not in (
    select ({{ unique_key }})
    from {{ tmp_relation }}
  )
{%- endmacro %}

{% macro clickhouse__incremental_insert_from_table(tmp_relation, target_relation) %}
  {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
  {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}

  insert into {{ target_relation }} ({{ dest_cols_csv }})
  select {{ dest_cols_csv }}
  from {{ tmp_relation }}
{%- endmacro %}

{% macro clickhouse__incremental_insert(target_relation, sql) %}
  {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
  {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}

  insert into {{ target_relation }} ({{ dest_cols_csv }})
  {{ sql }}
{%- endmacro %}

{% macro incremental_validate_on_schema_change(on_schema_change, default='ignore') %}
   
   {% if on_schema_change not in ['sync_all_columns', 'append_new_columns', 'fail', 'ignore'] %}
     
     {% set log_message = 'Invalid value for on_schema_change (%s) specified. Setting default value of %s.' % (on_schema_change, default) %}
     {% do log(log_message) %}
     
     {{ return(default) }}

   {% else %}

     {{ return(on_schema_change) }}
   
   {% endif %}

{% endmacro %}

{% macro process_schema_changes(on_schema_change, source_relation, target_relation) %}

    {% if on_schema_change != 'ignore' %}

      {% set schema_changes_dict = check_for_schema_changes(source_relation, target_relation) %}

      {% if schema_changes_dict['schema_changed'] %}

        {% if on_schema_change == 'fail' %}

          {% set fail_msg %}
              The source and target schemas on this incremental model are out of sync!
              They can be reconciled in several ways:
                - set the `on_schema_change` config to either append_new_columns or sync_all_columns, depending on your situation.
                - Re-run the incremental model with `full_refresh: True` to update the target schema.
                - update the schema manually and re-run the process.
          {% endset %}

          {% do exceptions.raise_compiler_error(fail_msg) %}

        {# -- unless we ignore, run the sync operation per the config #}
        {% else %}

          {% do sync_column_schemas(on_schema_change, target_relation, schema_changes_dict) %}

        {% endif %}

      {% endif %}

    {% endif %}

{% endmacro %}