{%- materialization dictionary, adapter='clickhouse' -%}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='dictionary') -%}
  {%- set intermediate_relation = make_intermediate_relation(target_relation) -%}
  {%- set existing_intermediate_relation = load_cached_relation(intermediate_relation) -%}
  {%- set backup_relation_type = 'dictionary' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
  {%- set existing_backup_relation = load_cached_relation(backup_relation) -%}
  {%- set cluster_clause = on_cluster_clause(target_relation) -%}

  {%- set grant_config = config.get('grants') -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {{ drop_dictionary_if_exists(existing_backup_relation, cluster_clause) }}
  {{ drop_dictionary_if_exists(existing_intermediate_relation, cluster_clause) }}


  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {# create our new dictionary #}
  {% call statement('main') -%}
    {{ clickhouse__get_create_dictionary_as_sql(intermediate_relation, cluster_clause, sql) }}
  {%- endcall %}

  {# cleanup #}
  {% if existing_relation is not none %}
    {% set existing_relation = load_cached_relation(existing_relation) %}
    {% if existing_relation is not none %}
      {{ adapter.rename_relation(existing_relation, backup_relation) }}
    {% endif %}
  {% endif %}
  {{ adapter.rename_relation(intermediate_relation, target_relation) }}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ adapter.commit() }}

  {{ drop_dictionary_if_exists(backup_relation, cluster_clause) }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization -%}


{% macro clickhouse__get_create_dictionary_as_sql(relation, cluster_clause, sql) %}
  {%- set fields = config.get('fields') -%}
  {%- set source_type = config.get('source_type') -%}

  CREATE DICTIONARY {{ relation }} {{ cluster_clause }}
  (
  {%- for (name, data_type) in fields -%}
    {{ name }} {{ data_type }}{%- if not loop.last -%},{%- endif -%}
  {%- endfor -%}
  )
  {{ primary_key_clause(label="primary key") }}
  SOURCE(
    {%- if source_type == 'http' %}
      {{ http_source() }}
    {% else %}
      {{ clickhouse_source(sql) }}
    {% endif -%}
  )
  LAYOUT({{ config.get('layout') }})
  LIFETIME({{ config.get('lifetime') }})
{% endmacro %}


{% macro http_source() %}
  HTTP(URL '{{ config.get("url") }}' FORMAT '{{ config.get("format") }}')
{% endmacro %}


{% macro clickhouse_source(sql) %}
  {%- set credentials = adapter.get_credentials(config.get("connection_overrides", {})) -%}
  {%- set table = config.get('table') -%}
  CLICKHOUSE(
      {% if credentials.get("user") -%}
      user '{{ credentials.get("user") }}'
      {%- endif %}
      {% if credentials.get("password") -%}
      password '{{ credentials.get("password") }}'
      {%- endif %}
      {% if credentials.get("database") -%}
      db '{{ credentials.get("database") }}'
      {%- endif %}
      {%- if table is not none %}
        table '{{ table }}'
      {% else %}
        query "{{ sql }}"
      {% endif -%}
  )
{% endmacro %}


{% macro drop_dictionary_if_exists(relation, cluster_clause) %}
  {% if relation.type != 'dictionary' %}
    {{ log(relation ~ ' is not a dictionary; defaulting to drop_relation_if_exists') }}
    {{ drop_relation_if_exists(relation) }}
  {% else %}
    {% call statement('drop_dictionary_if_exists') %}
      drop dictionary if exists {{ relation }} {{ cluster_clause }}
    {% endcall %}
  {% endif %}
{% endmacro %}
