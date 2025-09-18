{% macro clickhouse__get_assert_columns_equivalent(sql) -%}
  {%- set user_defined_columns = model['columns'] -%}

  {%- if not user_defined_columns -%}
      {{ exceptions.raise_contract_error([], []) }}
  {%- endif -%}

  {%- set yaml_columns = user_defined_columns.values() -%}

  {%- set sql_file_provided_columns = adapter.get_column_schema_from_query(sql) -%}
  {%- set sql_columns = adapter.format_columns(sql_file_provided_columns) -%}

  {%- if sql_columns|length != yaml_columns|length -%}
    {%- do exceptions.raise_contract_error(yaml_columns, sql_columns) -%}
  {%- endif -%}

  {%- if sql_columns|length != yaml_columns|length -%}
    {%- do exceptions.raise_contract_error(yaml_columns, sql_columns) -%}
  {%- endif -%}

  {%- for sql_col in sql_columns -%}
    {%- set yaml_col = [] -%}
    {%- for this_col in yaml_columns -%}
      {%- if this_col['name'] == sql_col['name'] -%}
        {%- do yaml_col.append(this_col) -%}
        {%- break -%}
      {%- endif -%}
    {%- endfor -%}
    {%- if not yaml_col -%}
      {#-- Column with name not found in yaml #}
      {%- do exceptions.raise_contract_error(yaml_columns, sql_columns) -%}
    {%- endif -%}

    {%- set yaml_data_type = yaml_col[0]['data_type'] -%}
    {%- set sql_data_type = sql_col['data_type'] -%}

    {%- set ns = namespace(is_special_type=false) -%}

    {%- set special_types = ['SimpleAggregateFunction'] -%}
    {%- for special_type in special_types -%}
      {%- if yaml_data_type.startswith(special_type) -%}
        {%- set ns.is_special_type = true -%}
        {%- break -%}
      {%- endif -%}
    {%- endfor -%}

    {% if not ns.is_special_type %}
      {%- if yaml_data_type.startswith('LowCardinality') -%}
        {#-- If contract is LowCardinality, extract the inner type for comparison --#}
        {%- set yaml_data_type = yaml_data_type[15:-1] -%}
      {%- endif -%}

      {%- if sql_data_type != yaml_data_type -%}
        {#-- Column data types don't match #}
        {%- do exceptions.raise_contract_error(yaml_columns, sql_columns) -%}
      {%- endif -%}
    {% endif %} 
  {%- endfor -%}

{% endmacro %}

