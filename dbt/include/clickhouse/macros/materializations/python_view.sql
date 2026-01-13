{% materialization python_view, adapter='clickhouse', supported_languages=['python'] %}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  
  {%- set build_sql = sql -%}
  
  {%- set result = adapter.submit_python_job(parsed_model, build_sql) -%}
  
  {% set target_relation = api.Relation.create(
      identifier=identifier,
      schema=schema,
      database=database,
      type='view') %}
  
  {%- if old_relation is not none -%}
      {%- do adapter.drop_relation(old_relation) -%}
  {%- endif -%}
  
  {%- do adapter.python_executor.materialize_dataframe(
      result, 
      identifier, 
      schema,
      'view',
      adapter.connections.get_thread_connection()
  ) -%}
  
  {% do return({'relations': [target_relation]}) %}

{% endmaterialization %}