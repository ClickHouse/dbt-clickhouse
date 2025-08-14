{% macro one_alter_relation(relation, alter_comments) %}
  alter table {{ relation }} {{ on_cluster_clause(relation) }} {{ alter_comments }}
{% endmacro %}

{% macro one_alter_column_comment(relation, column_name, comment) %}
  alter table {{ relation }} {{ on_cluster_clause(relation) }} comment column `{{ column_name }}` '{{ comment }}'
{% endmacro %}

{% macro clickhouse__alter_relation_comment(relation, comment) %}
  alter table {{ relation }} {{ on_cluster_clause(relation) }} modify comment '{{ comment }}'
{% endmacro %}

{% macro clickhouse__persist_docs(relation, model, for_relation, for_columns) -%}
  {# Persist table comment if enabled and description provided #}
  {%- if for_relation and config.persist_relation_docs() and model.description -%}
    {{ _persist_table_comment(relation, model.description) }}
  {%- endif -%}

  {#- Persist column comments if enabled and columns defined -#}
  {%- if for_columns and config.persist_column_docs() and model.columns -%}
    {{ _persist_column_comments(relation, model.columns) }}
  {%- endif -%}
{%- endmacro %}

{#- Helper macro: persist the table comment for a ClickHouse relation. -#}
{% macro _persist_table_comment(relation, description) -%}
  {#- Escape the description to be safe for ClickHouse #}
  {%- set escaped_comment = clickhouse_escape_comment(description) -%}
  {#- Build and run the ALTER TABLE ... MODIFY COMMENT statement -#}
  {%- set sql = "modify comment {comment}".format(comment=escaped_comment) -%}
  {{ run_query(one_alter_relation(relation, sql)) }}
{%- endmacro %}

{#- Helper macro: persist comments for multiple columns on a ClickHouse table.
 relation: target table relation
 columns: dict mapping column names to metadata (including 'description') -#}
{% macro _persist_column_comments(relation, columns) -%}
  {#- Gather existing columns in the relation to avoid altering non-existent ones -#}
  {%- set existing_columns = adapter.get_columns_in_relation(relation) | map(attribute="name") | list -%}
  {#- Collect ALTER statements for each column with a description -#}
  {%- set alterations = [] -%}
  {%- for column_name, info in columns.items() if info.description and column_name in existing_columns -%}
    {%- set escaped_comment = clickhouse_escape_comment(info.description) -%}
    {%- do alterations.append("\ncomment column `{column_name}` {comment}".format(column_name=column_name, comment=escaped_comment)) -%}
  {%- endfor -%}
  {#- Execute a single ALTER TABLE statement for all column comments -#}
  {%- if alterations -%}
    {{ run_query(one_alter_relation(relation, alterations | join(", "))) }}
  {%- endif -%}
{%- endmacro %}


{#
  By using dollar-quoting like this, users can embed anything they want into their comments
  (including nested dollar-quoting), as long as they do not use this exact dollar-quoting
  label. It would be nice to just pick a new one but eventually you do have to give up.
#}
{% macro clickhouse_escape_comment(comment) -%}
  {% if adapter.is_before_version('21.9.2.17') %}
    {% do exceptions.raise_compiler_error('Unsupported ClickHouse version for using heredoc syntax') %}
  {% endif %}
  {% if comment is not string %}
    {% do exceptions.raise_compiler_error('cannot escape a non-string: ' ~ comment) %}
  {% endif %}
  {%- set magic = '$dbt_comment_literal_block$' -%}
  {%- if magic in comment -%}
    {%- do exceptions.raise_compiler_error('The string ' ~ magic ~ ' is not allowed in comments.') -%}
  {%- endif -%}
  {{ magic }}{{ comment }}{{ magic }}
{%- endmacro %}
