{% macro clickhouse__load_csv_rows(model, agate_table) %}
  {% do adapter.insert_table_data(this.render(), agate_table) %}
{% endmacro %}

{% macro clickhouse__create_csv_table(model, agate_table) %}
  {%- set column_override = model['config'].get('column_types', {}) -%}
  {%- set quote_seed_column = model['config'].get('quote_columns', None) -%}

  {% set sql %}
    create table {{ this.render() }} (
      {%- for col_name in agate_table.column_names -%}
        {%- set inferred_type = adapter.convert_type(agate_table, loop.index0) -%}
        {%- set type = column_override.get(col_name, inferred_type) -%}
        {%- set column_name = (col_name | string) -%}
          {{ adapter.quote_seed_column(column_name, quote_seed_column) }} {{ type }} {%- if not loop.last -%}, {%- endif -%}
      {%- endfor -%}
    )
    {{ engine_clause(label='engine') }}
    {{ order_cols(label='order by') }}
    {{ partition_cols(label='partition by') }}
  {% endset %}

  {% call statement('_') -%}
    {{ sql }}
  {%- endcall %}

  {{ return(sql) }}
{% endmacro %}
