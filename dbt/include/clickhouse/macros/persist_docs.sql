{% macro one_alter_column_comment(relation, column_name, comment) %}
  alter table {{ relation }} comment column {{ column_name }} '{{ comment }}'
{% endmacro %}

{% macro clickhouse__alter_relation_comment(relation, comment) %}
  alter table {{ relation }} modify comment '{{ comment }}'
{% endmacro %}

{% macro clickhouse__persist_docs(relation, model, for_relation, for_columns) %}
  {%- if for_relation and config.persist_relation_docs() and model.description -%}
    {% do run_query(alter_relation_comment(relation, model.description)) %}
  {%- endif -%}

  {%- if for_columns and config.persist_column_docs() and model.columns -%}
    {%- for column_name in model.columns -%}
      {%- set comment = model.columns[column_name]['description'] -%}
      {% do run_query(one_alter_column_comment(relation, column_name, comment)) %}
    {%- endfor -%}
  {%- endif -%}
{% endmacro %}