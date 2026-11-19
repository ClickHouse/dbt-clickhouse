{% macro clickhouse__formatted_scalar_function_args_sql() -%}
    {%- for arg in model.arguments if arg.default_value is defined and arg.default_value is not none -%}
        {%- do exceptions.raise_compiler_error("Argument `" ~ arg.name ~ "` of function `" ~ model.name ~ "` has a default value, which ClickHouse SQL UDFs do not support. Remove `default_value` and pass the argument explicitly.") -%}
    {%- endfor -%}

    {{ model.arguments | map(attribute='name') | join(', ') }}
{%- endmacro %}

{% macro clickhouse__scalar_function_create_replace_signature_sql(target_relation) -%}
    {%- do scalar_function_volatility_sql() -%}
    CREATE OR REPLACE FUNCTION {{ target_relation }} {{ on_cluster_clause(target_relation) }} AS ({{ formatted_scalar_function_args_sql() }}) ->
{%- endmacro %}

{% macro clickhouse__scalar_function_body_sql() -%}
    {{ model.compiled_code }}
{%- endmacro %}

{% macro clickhouse__scalar_function_volatility_sql() %}
    {% set volatility = model.config.get('volatility') %}
    {% if volatility != None %}
        {% do unsupported_volatility_warning(volatility) %}
    {% endif %}
{% endmacro %}
