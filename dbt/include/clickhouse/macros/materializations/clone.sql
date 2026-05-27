{% macro clickhouse__can_clone_table() %}
    {{ return(True) }}
{% endmacro %}

{% macro clickhouse__create_or_replace_clone(this_relation, defer_relation) %}
    create or replace table {{ this_relation.render() }} {{ on_cluster_clause(this_relation)}} as {{ defer_relation.render() }}
{% endmacro %}
