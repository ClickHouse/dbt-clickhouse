{%- materialization view -%}

  {% set target_relation = this.incorporate(type='view') %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}


  {{ drop_relation_if_exists(target_relation) }}


  -- build model
  {% do run_query(create_view_as(target_relation, sql)) %}

  -- cleanup
  -- move the existing view out of the way
  

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ drop_relation_if_exists(backup_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization -%}