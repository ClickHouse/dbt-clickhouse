{% macro clickhouse__can_clone_table() %}
    {#
        Clone behavior with ClickHouse (dbt clone only operates on 'table'-type materializations, for the rest it falls back to view):
        - *MergeTree tables  -> cloned via `CLONE AS`.
        - Tables with other engines -> not supported, fall back to a view.
        - Distributed tables -> not supported, fall back to a view.
        - Tables created ON CLUSTER -> not supported, fall back to a view. The initiator queues the
          normalized CREATE without the CLONE AS source, so every host created an empty table, and
          ClickHouse 26.11+ rejects CLONE AS with ON CLUSTER (SUPPORT_IS_DISABLED).
        - Materialized views -> only the target table is cloned; now MV is crated/attached to the clon.
    #}
    {%- if config.get('materialized', '').startswith('distributed_') -%}
        {{ return(False) }}
    {%- endif -%}
    {%- if adapter.get_clickhouse_cluster_name() is not none and adapter.should_on_cluster(relation=this) -%}
        {#- A clone onto the deferred relation itself stays on the table path: dbt-core skips it
            there without running CLONE AS, while its view path would replace the table with a
            view of itself. -#}
        {%- set is_self_clone = defer_relation is not none
            and this.database == defer_relation.database
            and this.schema == defer_relation.schema
            and this.identifier == defer_relation.identifier -%}
        {%- if not is_self_clone -%}
            {{ return(False) }}
        {%- endif -%}
    {%- endif -%}
    {%- set engine = config.get('engine', default='MergeTree()') -%}
    {{ return('MergeTree' in engine) }}
{% endmacro %}

{% macro clickhouse__create_or_replace_clone(this_relation, defer_relation) %}
    CREATE OR REPLACE TABLE {{ this_relation }} {{ on_cluster_clause(this_relation) }} CLONE AS {{ defer_relation }}
{% endmacro %}
