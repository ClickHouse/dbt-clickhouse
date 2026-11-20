{% macro clickhouse__can_clone_table() %}
    {#
        Clone behavior with ClickHouse (dbt clone only operates on 'table'-type materializations, for the rest it falls back to view):
        - *MergeTree tables  -> cloned, see `clickhouse__create_or_replace_clone`.
        - Tables with other engines -> not supported, fall back to a view.
        - Distributed tables -> not supported, fall back to a view.
        - Materialized views -> only the target table is cloned; the MV is then created on top of the clone.
    #}
    {%- if config.get('materialized', '').startswith('distributed_') -%}
        {{ return(False) }}
    {%- endif -%}
    {%- set engine = config.get('engine', default='MergeTree()') -%}
    {%- if 'MergeTree' not in engine -%}
        {{ return(False) }}
    {%- endif -%}
    {{ return(True) }}
{% endmacro %}

{% macro clickhouse__create_or_replace_clone(this_relation, defer_relation) %}
    {%- set database = adapter.get_ch_database(this_relation.schema) -%}
    {#- `CLONE AS` drops the source `ON CLUSTER`, which creates empty tables, and Replicated databases reject it -#}
    {%- if (adapter.get_clickhouse_cluster_name() is not none and adapter.should_on_cluster(relation=this_relation))
          or (database and database.engine == 'Replicated') -%}
        {%- set on_cluster = on_cluster_clause(this_relation) -%}
        {% call statement('create_clone_table') %}
            CREATE OR REPLACE TABLE {{ this_relation }} {{ on_cluster }} AS {{ defer_relation }}
        {% endcall %}
        ALTER TABLE {{ this_relation }} {{ on_cluster }} ATTACH PARTITION ALL FROM {{ defer_relation }}
    {%- else -%}
        CREATE OR REPLACE TABLE {{ this_relation }} CLONE AS {{ defer_relation }}
    {%- endif -%}
{% endmacro %}
