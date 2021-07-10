{% macro incremental_upsert(tmp_relation, target_relation, unique_key=none, statement_name='main') %}
  {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
  {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}


  insert into {{ target_relation }} ({{ dest_cols_csv }})
    select {{ dest_cols_csv }}
    from {{ tmp_relation }};
{%- endmacro %}
