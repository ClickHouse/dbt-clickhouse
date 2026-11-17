{% macro clickhouse__function_execute_build_sql(build_sql, existing_relation, target_relation) %}
    {#- The default implementation would apply grants and persist docs, however ClickHouse doesnt
        have function usage grants or function comments. -#}

    {% call statement(name="main") %}
        {{ build_sql }}
    {% endcall %}

    {{ adapter.commit() }}
{% endmacro %}
