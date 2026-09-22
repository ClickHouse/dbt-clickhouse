{#
  Relation of the shard-local table behind a distributed model.
  Resolved from model config, then profile: local_db (as is) or local_db_prefix + schema.
#}
{% macro clickhouse_local_relation(base_relation=none, from_relation=none) -%}
  {%- set base = base_relation if base_relation is not none else this -%}

  {%- set schema = adapter.get_clickhouse_local_db(config.get('local_db')) -%}
  {%- if not schema -%}
    {%- set schema = adapter.get_clickhouse_local_db_prefix(config.get('local_db_prefix')) ~ base.schema -%}
  {%- endif -%}
  {%- set identifier = base.identifier ~ adapter.get_clickhouse_local_suffix(config.get('local_suffix')) -%}

  {%- if schema == base.schema and identifier == base.identifier -%}
    {% do exceptions.raise_compiler_error(
      'The local table of ' ~ base ~ ' resolves to the Distributed table itself.') %}
  {%- endif -%}

  {%- set source = from_relation if from_relation is not none else base -%}
  {{ return(source.incorporate(path={"identifier": identifier, "schema": schema})) }}
{%- endmacro %}
