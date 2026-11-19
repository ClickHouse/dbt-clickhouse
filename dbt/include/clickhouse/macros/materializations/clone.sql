{% macro clickhouse__can_clone_table() %}
    {#
        Clone behavior with ClickHouse (dbt clone only operates on 'table'-type materializations, for the rest it falls back to view):
        - *MergeTree tables  -> cloned via `CLONE AS`.
        - Tables with other engines -> not supported, fall back to a view.
        - Distributed tables -> not supported, fall back to a view.
        - Tables created `ON CLUSTER` or in a Replicated database -> not supported by `CLONE AS`, fall back to a view.
        - Materialized views -> only the target table is cloned; the MV is then created on top of the clone.
    #}
    {#- dbt only skips cloning a table onto itself when it can clone, the view fallback would select from itself -#}
    {%- if defer_relation and this.schema == defer_relation.schema and this.identifier == defer_relation.identifier -%}
        {{ return(True) }}
    {%- endif -%}
    {%- if config.get('materialized', '').startswith('distributed_') -%}
        {{ return(False) }}
    {%- endif -%}
    {%- set engine = config.get('engine', default='MergeTree()') -%}
    {%- if 'MergeTree' not in engine -%}
        {{ return(False) }}
    {%- endif -%}
    {#- ClickHouse drops the source from `CLONE AS ... ON CLUSTER`, which creates empty tables or fails -#}
    {%- if adapter.get_clickhouse_cluster_name() is not none and adapter.should_on_cluster(relation=this) -%}
        {{ return(False) }}
    {%- endif -%}
    {%- set database = adapter.get_ch_database(this.schema) -%}
    {%- if database and database.engine == 'Replicated' -%}
        {{ return(False) }}
    {%- endif -%}
    {{ return(True) }}
{% endmacro %}

{% macro clickhouse__create_or_replace_clone(this_relation, defer_relation) %}
    CREATE OR REPLACE TABLE {{ this_relation }} CLONE AS {{ defer_relation }}
{% endmacro %}
