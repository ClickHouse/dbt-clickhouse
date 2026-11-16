{#- ClickHouse's dateDiff only takes Date, Date32, DateTime and DateTime64 arguments, while
    dbt documents string literals such as "'2016-03-09'" as datediff arguments. A literal
    is read as a DateTime64(6), so a date or a timestamp with sub-second digits both work.
    Columns and other expressions are passed through unchanged. -#}
{%- macro clickhouse__datediff(first_date, second_date, datepart) -%}
    dateDiff(
        {{ datepart }},
        {{ clickhouse__datediff_argument(first_date) }},
        {{ clickhouse__datediff_argument(second_date) }}
    )
{%- endmacro -%}


{%- macro clickhouse__datediff_argument(date_expression) -%}
    {%- set expression = date_expression | string | trim -%}
    {%- if expression | length > 1 and expression.startswith("'") and expression.endswith("'") -%}
        toDateTime64({{ expression }}, 6)
    {%- else -%}
        {{ date_expression }}
    {%- endif -%}
{%- endmacro -%}
