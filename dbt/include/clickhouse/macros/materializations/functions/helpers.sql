{% macro clickhouse__function_execute_build_sql(build_sql, existing_relation, target_relation) %}
    {#- skips persist_docs, since ClickHouse has no function comments -#}

    {#- dbt reruns this materialization per overload under the same name, so each would replace the last -#}
    {%- if model.overloads -%}
        {%- do exceptions.raise_compiler_error("Function `" ~ model.name ~ "` defines overloads, which ClickHouse does not support. Use a separately named function per signature instead.") -%}
    {%- endif -%}

    {%- if config.get('grants') -%}
        {%- do exceptions.raise_compiler_error("Function `" ~ model.name ~ "` has grants configured, but ClickHouse has no grants for functions and any user can call them. Remove `grants` from the function config.") -%}
    {%- endif -%}

    {% call statement(name="main") %}
        {{ build_sql }}
    {% endcall %}

    {{ adapter.commit() }}
{% endmacro %}
