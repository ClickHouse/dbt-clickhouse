{% macro clickhouse__function_execute_build_sql(build_sql, existing_relation, target_relation) %}
    {#- skips grants and persist_docs. ClickHouse has no function grants or comments. -#}

    {#- dbt reruns this materialization per overload under the same name, so each would replace the last -#}
    {%- if model.overloads -%}
        {%- do exceptions.raise_compiler_error("Function `" ~ model.name ~ "` defines overloads, which ClickHouse does not support. Use a separately named function per signature instead.") -%}
    {%- endif -%}

    {%- if config.get('grants') -%}
        {%- do exceptions.warn("Function `" ~ model.name ~ "` has grants configured, which ClickHouse does not support for functions. They will be ignored and any user can call the function.") -%}
    {%- endif -%}

    {% call statement(name="main") %}
        {{ build_sql }}
    {% endcall %}

    {{ adapter.commit() }}
{% endmacro %}
