{% materialization python_table, adapter='clickhouse', supported_languages=['python'] %}

  {%- set identifier = model.alias -%}
  {%- set schema = model.schema -%}
  {%- set database = model.database -%}
  
  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  
  {% set target_relation = api.Relation.create(
      identifier=identifier,
      schema=schema,
      database=database,
      type='table') %}
  
  {%- if old_relation is not none -%}
      {%- do adapter.drop_relation(old_relation) -%}
  {%- endif -%}
  
  -- Execute Python model using the adapter's submit_python_job method
  -- Pass 'model' (the full model dictionary) and 'compiled_code' (the compiled Python code)
  {%- set python_result = adapter.submit_python_job(model, compiled_code) -%}
  
  -- Store the result so dbt-core can track it
  {%- do store_result('main', python_result) -%}
  
  {% do return({'relations': [target_relation]}) %}

{% endmaterialization %}
