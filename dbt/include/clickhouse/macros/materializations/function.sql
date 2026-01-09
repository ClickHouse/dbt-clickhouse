{%- materialization function, adapter='clickhouse' -%}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='function') -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {% if existing_relation is none %}
    {{ log('Creating new function ' ~ target_relation.identifier) }}
  {% else %}
    {{ log('Function ' ~ target_relation.identifier ~ ' exists, replacing it') }}
  {% endif %}

  {# create our new function #}
  {% call statement('main') -%}
    {{ clickhouse__scalar_function_sql(target_relation) }}
  {%- endcall %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization -%}
