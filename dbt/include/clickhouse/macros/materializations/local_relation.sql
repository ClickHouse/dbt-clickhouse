{% macro clickhouse_local_relation(base_relation=none, from_relation=none) -%}
  {%- set base = base_relation if base_relation is not none else this -%}

  {%- set schema = adapter.get_clickhouse_local_db_prefix() ~ base.schema -%}
  {%- set identifier = base.identifier ~ adapter.get_clickhouse_local_suffix() -%}

  {%- set source = from_relation if from_relation is not none else base -%}
  {{ return(source.incorporate(path={"identifier": identifier, "schema": schema})) }}
{%- endmacro %}
