{% macro clickhouse__apply_column_changes(column_changes, existing_relation, is_distributed=False) %}
    {{ log('Schema changes detected. Trying to apply the following changes: ' ~ column_changes) }}
    {%- set existing_local = none -%}
    {% if is_distributed %}
        {%- set existing_local = clickhouse_local_relation(this, existing_relation) if existing_relation is not none else none -%}
    {% endif %}

    {% if column_changes.on_schema_change == 'append_new_columns' %}
        {% do clickhouse__add_columns(column_changes.columns_to_add, existing_relation, existing_local) %}

    {% elif column_changes.on_schema_change == 'sync_all_columns' %}
        {% do clickhouse__drop_columns(column_changes.columns_to_drop, existing_relation, existing_local) %}
        {% do clickhouse__add_columns(column_changes.columns_to_add, existing_relation, existing_local) %}
        {% do clickhouse__modify_columns(column_changes.columns_to_modify, existing_relation, existing_local) %}
    {% endif %}

{% endmacro %}

{% macro column_codec_clause(column_name) -%}
    {%- if config.get('contract').enforced -%}
        {{ codec_clause(model['columns'].get(column_name, {}).get('codec')) }}
    {%- endif -%}
{%- endmacro %}

{% macro column_ttl_clause(column_name) -%}
    {%- if config.get('contract').enforced -%}
        {{ ttl_clause(model['columns'].get(column_name, {}).get('ttl')) }}
    {%- endif -%}
{%- endmacro %}

{% macro exec_alter_table(relation, action, on_cluster='') %}
    {% call statement('alter_table') %}
        alter table {{ relation }} {{ on_cluster }} {{ action }}
    {% endcall %}

{% endmacro %}

{% macro clickhouse__add_columns(columns, existing_relation, existing_local=none) %}
    {% do clickhouse__alter_columns('add column if not exists', columns, existing_relation, existing_local) %}

{% endmacro %}

{% macro clickhouse__drop_columns(columns, existing_relation, existing_local=none) %}
    {% if columns | length == 0 %}
        {% do return(none) %}
    {% endif %}
    {% set actions = [] %}
    {% for column in columns %}
        {% do actions.append('drop column if exists `' ~ column.name ~ '`') %}
    {% endfor %}
    {% do clickhouse__run_alter_table_command(actions | join(', '), existing_relation, existing_local) %}

{% endmacro %}

{% macro clickhouse__modify_columns(columns, existing_relation, existing_local=none) %}
    {% do clickhouse__alter_columns('modify column if exists', columns, existing_relation, existing_local) %}

{% endmacro %}

{% macro clickhouse__alter_columns(command, columns, existing_relation, existing_local=none) %}
    {% if columns | length == 0 %}
        {% do return(none) %}
    {% endif %}
    {% set local_actions = [] %}
    {% set distributed_actions = [] %}
    {% for column in columns %}
        {% set decl = command ~ ' `' ~ column.name ~ '` ' ~ column.data_type ~ ' ' ~ column_codec_clause(column.name) %}
        {% do local_actions.append(decl ~ ' ' ~ column_ttl_clause(column.name)) %}
        {% do distributed_actions.append(decl) %}
    {% endfor %}
    {% set distributed_action = distributed_actions | join(', ') if existing_local is not none else none %}
    {% do clickhouse__run_alter_table_command(local_actions | join(', '), existing_relation, existing_local, distributed_action) %}

{% endmacro %}

{% macro clickhouse__run_alter_table_command(local_action, existing_relation, existing_local=none, distributed_action=none) %}
    {% if existing_local is not none %}
        {% do exec_alter_table(existing_local, local_action, on_cluster_clause(existing_relation)) %}
        {% do exec_alter_table(existing_relation, distributed_action or local_action, on_cluster_clause(existing_relation)) %}
    {% else %}
        {% do exec_alter_table(existing_relation, local_action) %}
    {% endif %}

{% endmacro %}
