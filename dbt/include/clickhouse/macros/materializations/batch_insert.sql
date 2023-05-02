{% macro is_batch_insert() -%}
    {% if not execute %}
        {{ return(False) }}
    {% else %}
        {{ return(model.config.batch_insert or False) }}
    {% endif %}
{%- endmacro %}

{% macro query_batch_insert_seeds(sql) -%}
    {% call statement("batch_insert_seeds", fetch_result=True) %}
        {{ sql }}
    {%- endcall %}

    {% if execute %}
        {% set results = load_result('batch_insert_seeds') %}
        {{ return(results.table.rows) }}
    {% else %}
        {{ return([none]) }}
    {% endif %}
{%- endmacro %}

{% macro iterate_batch_insert_seeds(sql_seeds) -%}
    {{ config.set('batch_insert', True) }}
    {%- set target_relation = this -%}
    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set is_creating_table = dest_columns|length == 0 -%}

    {% if not execute %}
        {{ caller(none) }}
    {% elif is_creating_table %}
        {%- set seeds = query_batch_insert_seeds(sql_seeds) -%}
        {{ caller(seeds|first) }}

        {% for seed in seeds %}
            {% if not loop.first %}
                /!-MAGIC:BATCH_INSERT
                {{ caller(seed) }}
                MAGIC:BATCH_INSERT-!/
            {% endif %}
        {% endfor %}
    {% else %}
        {%- set seeds = query_batch_insert_seeds(sql_seeds) -%}
        {{ caller(seeds|first) }}
    {% endif %}
{%- endmacro %}

{% macro batch_insert_mask(sql) -%}
    {% if is_batch_insert() %}
        {% set re = modules.re %}
        {% set parsed = re.sub('\/!\-MAGIC:BATCH_INSERT(.+?)MAGIC:BATCH_INSERT\-!\/', '', sql, flags=re.DOTALL) %}
        {{ return(parsed) }}
    {% else %}
        {{ return(sql) }}
    {% endif %}
{%- endmacro %}

{% macro batch_insert_unmask(sql, insert_into_sql) -%}
    {% if is_batch_insert() %}
        {% set re = modules.re %}
        {% set parsed = re.sub('\/!\-MAGIC:BATCH_INSERT(.+?)MAGIC:BATCH_INSERT\-!\/', '; ' ~ insert_into_sql ~ ' \\1', sql, flags=re.DOTALL) %}
        {{ return(parsed) }}
    {% else %}
        {{ return(sql) }}
    {% endif %}
{%- endmacro %}
