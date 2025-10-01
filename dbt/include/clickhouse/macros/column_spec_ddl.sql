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

    {%- set ns = namespace(yaml_data_type=yaml_col[0]['data_type']) -%}
    {%- set sql_data_type = sql_col['data_type'] -%}

    {#--
        A list of ClickHouse types where we only care about the inner type for comparison.
        The first element is the type name (case-sensitive).
        The second is the zero-based index of the argument to extract for comparison.
        For example SimpleAggregateFunction(max, UInt8) has two arguments, we want to use the UInt8,
        i.e. the second argument (index 1) for comparison.
    --#}
    {%- set wrapper_types = [
        ('SimpleAggregateFunction', 1),
        ('LowCardinality', 0)
    ] -%}

    {%- set unwrapping = true -%}
    {%- for _ in range(10) if unwrapping -%} {#-- Limit to 10 iterations to avoid infinite loops --#}
      {%- set unwrapping = false -%}
      {%- for type_name, arg_index in wrapper_types -%}
        {%- if ns.yaml_data_type.startswith(type_name) -%}
          {%- set ns.yaml_data_type = ns.yaml_data_type.split('(', 1)[1].rsplit(')', 1)[0] -%}
          {%- set ns.yaml_data_type = ns.yaml_data_type.split(',')[arg_index].strip() -%}
          {%- set unwrapping = true -%}
          {%- break -%}
        {%- endif -%}
      {%- endfor -%}
    {%- endfor -%}

    {%- if sql_data_type != ns.yaml_data_type -%}
      {#-- Column data types don't match #}
        {%- do exceptions.raise_contract_error(yaml_columns, sql_columns) -%}
      {%- endif -%}
    {%- endfor -%}

{% endmacro %}

