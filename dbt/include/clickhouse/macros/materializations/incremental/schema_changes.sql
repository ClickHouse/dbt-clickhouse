{% macro clickhouse__apply_column_changes(column_changes, existing_relation, is_distributed=False) %}
    {{ log('Schema changes detected. Trying to apply the following changes: ' ~ column_changes) }}
    {%- set existing_local = none -%}
    {% if is_distributed %}
        {%- set local_suffix = adapter.get_clickhouse_local_suffix() -%}
        {%- set local_db_prefix = adapter.get_clickhouse_local_db_prefix() -%}
        {%- set existing_local = existing_relation.incorporate(path={"identifier": this.identifier + local_suffix, "schema": local_db_prefix + this.schema}) if existing_relation is not none else none -%}
    {% endif %}

    {% if column_changes.on_schema_change == 'append_new_columns' %}
        {% do clickhouse__add_columns(column_changes.columns_to_add, existing_relation, existing_local, is_distributed) %}

    {% elif column_changes.on_schema_change == 'sync_all_columns' %}
        {% do clickhouse__drop_columns(column_changes.columns_to_drop, existing_relation, existing_local, is_distributed) %}
        {% do clickhouse__add_columns(column_changes.columns_to_add, existing_relation, existing_local, is_distributed) %}
        {% do clickhouse__modify_columns(column_changes.columns_to_modify, existing_relation, existing_local, is_distributed) %}
    {% endif %}

{% endmacro %}

{% macro column_codec_clause(column_name) -%}
    {{ codec_clause(model['columns'].get(column_name, {}).get('codec')) }}
{%- endmacro %}

{% macro column_ttl_clause(column_name) -%}
    {{ ttl_clause(model['columns'].get(column_name, {}).get('ttl')) }}
{%- endmacro %}

{% macro exec_alter_table(relation, action, on_cluster='') %}
    {% call statement('alter_table') %}
        alter table {{ relation }} {{ on_cluster }} {{ action }}
    {% endcall %}

{% endmacro %}

{% macro clickhouse__add_columns(columns, existing_relation, existing_local=none, is_distributed=False) %}
    {% set command = 'add column if not exists' %}
    {% for column in columns %}
        {% set decl = '`' ~ column.name ~ '` ' ~ column.data_type %}
        {% set local_action = command ~ ' ' ~ decl ~ ' ' ~ column_codec_clause(column.name) ~ ' ' ~ column_ttl_clause(column.name) %}
        {% set distributed_action = command ~ ' ' ~ decl ~ ' ' ~ column_codec_clause(column.name) %}
        {% do clickhouse__run_alter_table_command(local_action, existing_relation, existing_local, is_distributed, distributed_action) %}
    {% endfor %}

{% endmacro %}

{% macro clickhouse__drop_columns(columns, existing_relation, existing_local=none, is_distributed=False) %}
    {% for column in columns %}
        {% set alter_action -%}
            drop column if exists `{{ column.name }}`
        {%- endset %}
        {% do clickhouse__run_alter_table_command(alter_action, existing_relation, existing_local, is_distributed) %}
    {% endfor %}

{% endmacro %}

{% macro clickhouse__modify_columns(columns, existing_relation, existing_local=none, is_distributed=False) %}
    {% set command = 'modify column if exists' %}
    {% for column in columns %}
        {% set decl = '`' ~ column.name ~ '` ' ~ column.data_type %}
        {% set local_action = command ~ ' ' ~ decl ~ ' ' ~ column_codec_clause(column.name) ~ ' ' ~ column_ttl_clause(column.name) %}
        {% set distributed_action = command ~ ' ' ~ decl ~ ' ' ~ column_codec_clause(column.name) %}
        {% do clickhouse__run_alter_table_command(local_action, existing_relation, existing_local, is_distributed, distributed_action) %}
    {% endfor %}

{% endmacro %}

{% macro clickhouse__run_alter_table_command(local_action, existing_relation, existing_local=none, is_distributed=False, distributed_action=none) %}
    {% if is_distributed %}
        {% do exec_alter_table(existing_local, local_action, on_cluster_clause(existing_relation)) %}
        {% do exec_alter_table(existing_relation, distributed_action or local_action, on_cluster_clause(existing_relation)) %}
    {% else %}
        {% do exec_alter_table(existing_relation, local_action) %}
    {% endif %}

{% endmacro %}
