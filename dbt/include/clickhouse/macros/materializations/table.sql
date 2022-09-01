{% materialization table, adapter='clickhouse' %}
  {%- set identifier = model['alias'] -%}
  {%- set tmp_identifier = model['name'] + '__dbt_tmp' -%}
  {%- set backup_identifier = model['name'] + '__dbt_backup' -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='table') -%}
  {%- set intermediate_relation = api.Relation.create(identifier=tmp_identifier,
                                                      schema=schema,
                                                      database=database,
                                                      type='table') -%}
  -- the intermediate_relation should not already exist in the database; get_relation
  -- will return None in that case. Otherwise, we get a relation that we can drop
  -- later, before we try to use this name for the current operation
  {%- set preexisting_intermediate_relation = adapter.get_relation(identifier=tmp_identifier,
                                                                   schema=schema,
                                                                   database=database) -%}
  /*
      See ../view/view.sql for more information about this relation.
  */
  {%- set backup_relation_type = 'table' if old_relation is none else old_relation.type -%}
  {%- set backup_relation = api.Relation.create(identifier=backup_identifier,
                                                schema=schema,
                                                database=database,
                                                type=backup_relation_type) -%}
  -- as above, the backup_relation should not already exist
  {%- set preexisting_backup_relation = adapter.get_relation(identifier=backup_identifier,
                                                             schema=schema,
                                                             database=database) -%}


  -- drop the temp relations if they exist already in the database
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% if is_atomic and old_relation is not none %}
    -- only makes sense to EXCHANGE when the relation already exists

    -- build model as backup_relation so it is saved after EXCHANGE
    {% call statement('main') -%}
      {{ get_create_table_as_sql(False, backup_relation, sql) }}
    {%- endcall %}

    {% do exchange_tables_atomic(backup_relation, old_relation) %}

  {% else %}

    -- build model
    {% call statement('main') -%}
      {{ get_create_table_as_sql(False, intermediate_relation, sql) }}
    {%- endcall %}

    -- cleanup, move the existing table out of the way and rename intermediate to target
    {% if old_relation is not none %}
      {{ adapter.rename_relation(old_relation, backup_relation) }}
    {% endif %}

    {{ adapter.rename_relation(intermediate_relation, target_relation) }}

  {% endif %}

  {% do create_indexes(target_relation) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {% do persist_docs(target_relation, model) %}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  -- finally, drop the existing/backup relation after the commit
  {{ drop_relation_if_exists(backup_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
