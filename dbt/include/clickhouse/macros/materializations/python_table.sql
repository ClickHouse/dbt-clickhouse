{# macros/materializations/python_table.sql #}

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
  
  {%- set full_refresh_mode = (should_full_refresh()) -%}
  {%- set exists_as_table = (old_relation is not none and old_relation.type == 'table') -%}
  
  {# For incremental, we need to handle it differently #}
  {%- if exists_as_table and not full_refresh_mode -%}
    {# Incremental run - temp table approach #}
    {%- set temp_relation = make_temp_relation(target_relation) -%}
    
    {# Execute Python model with incremental=True #}
    {%- set python_result = adapter.submit_python_job(model, compiled_code, is_incremental=true, target_relation=target_relation) -%}
    
    {# The adapter handles the merge/append logic #}
    {%- do store_result('main', python_result) -%}
    
  {%- else -%}
    {# Full refresh or first run #}
    {%- if old_relation is not none -%}
        {%- do adapter.drop_relation(old_relation) -%}
    {%- endif -%}
    
    {# Execute Python model with incremental=False #}
    {%- set python_result = adapter.submit_python_job(model, compiled_code, is_incremental=false, target_relation=target_relation) -%}
    
    {%- do store_result('main', python_result) -%}
    
  {%- endif -%}
  
  {% do return({'relations': [target_relation]}) %}

{% endmaterialization %}
