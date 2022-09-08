{% macro clickhouse__get_or_create_relation(database, schema, identifier, type) %}
  {%- set target_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) %}
  {% if target_relation %}
    {% do return([true, target_relation]) %}
  {% endif %}

  {% set ch_db = adapter.get_ch_database(schema) %}
  {%- set new_relation = api.Relation.create(
      database=None,
      schema=schema,
      identifier=identifier,
      type=type,
      db_engine=ch_db.engine if ch_db else ''
  ) -%}
  {% do return([false, new_relation]) %}
{% endmacro %}

{% macro clickhouse__get_database(database) %}
    {% call statement('get_database') %}
        select name, engine, comment
        from system.databases
        where name = '{{ database }}'
   {% endcall %}
   {% do return(load_result('get_database').table) %}
{% endmacro %}