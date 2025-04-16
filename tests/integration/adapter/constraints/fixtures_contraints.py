contract_model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: Int32
        description: hello
      - name: color
        data_type: String
      - name: date_day
        data_type: Date
  - name: my_model_error
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: Int32
        description: hello
        tests:
          - unique
      - name: color
        data_type: String
      - name: date_day
        data_type: Date
  - name: my_model_wrong_order
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: UInt32
        description: hello
        tests:
          - unique
      - name: color
        data_type: String
      - name: date_day
        data_type: Date
  - name: my_model_wrong_name
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: Int32
        description: hello
      - name: color
        data_type: String
      - name: date_day
        data_type: Date
"""


# model columns in a different order to schema definitions
my_model_wrong_order_sql = """
{{
  config(
    materialized = "table"
  )
}}

select
  'blue' as color,
  1::UInt32 as id,
  toDate('2019-01-01') as date_day
"""


# model columns name different to schema definitions
my_model_wrong_name_sql = """
{{
  config(
    materialized = "table"
  )
}}

select
  'blue' as color,
  1 as error,
  '2019-01-01' as date_day
"""


my_model_data_type_sql = """
{{{{
  config(
    materialized = "table"
  )
}}}}

select
  {sql_value} as wrong_data_type_column_name
"""


model_data_type_schema_yml = """
version: 2
models:
  - name: my_model_data_type
    config:
      contract:
        enforced: true
    columns:
      - name: wrong_data_type_column_name
        data_type: {data_type}
"""

my_model_view_wrong_name_sql = """
{{
  config(
    materialized = "view"
  )
}}

select
  'blue' as color,
  1 as error,
  toDate('2019-01-01') as date_day
"""

my_model_view_wrong_order_sql = """
{{
  config(
    materialized = "view"
  )
}}

select
  'blue' as color,
  1::UInt32 as id,
  toDate('2019-01-01') as date_day
"""


my_model_incremental_wrong_order_sql = """
{{
  config(
    materialized = "incremental",
    on_schema_change='append_new_columns'
  )
}}

select
  'blue' as color,
  1::UInt32 as id,
  toDate('2019-01-01') as date_day
"""

my_model_incremental_wrong_name_sql = """
{{
  config(
    materialized = "incremental",
    on_schema_change='append_new_columns'
  )
}}

select
  'blue' as color,
  1 as error,
  '2019-01-01' as date_day
"""

constraint_model_schema_yml = """
version: 2
models:
  - name: bad_column_constraint_model
    materialized: table
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: Int32
        constraints:
          - type: check
            expression: '> 0'
      - name: color
        data_type: String
      - name: date_day
        data_type: Date
  - name: bad_foreign_key_model
    config:
      contract:
        enforced: true
    constraints:
      - type: foreign_key
        columns: [ id ]
        expression: 'foreign_key_model (id)'
    columns:
      - name: id
        data_type: Int32
  - name: check_constraints_model
    config:
      contract:
        enforced: true
    constraints:
      - type: check
        name: valid_id
        expression: 'id > 100 and id < 200'
    columns:
      - name: id
        data_type: Int32
      - name: color
        data_type: String
      - name: date_day
        data_type: Date
"""

bad_column_constraint_model_sql = """
{{
  config(
    materialized = "table"
  )
}}

SELECT 5::Int32 as id, 'black' as color, toDate('2023-01-01') as date_day
"""

bad_foreign_key_model_sql = """
{{
  config(
    materialized = "table"
  )
}}

SELECT 1::Int32 as id
"""

check_constraints_model_sql = """
{{
  config(
    materialized = "table",
  )
}}

select
  'blue' as color,
  101::Int32 as id,
  toDate('2019-01-01') as date_day
"""

check_constraints_model_fail_sql = """
{{
  config(
    materialized = "table",
  )
}}

select
  'blue' as color,
  1::Int32 as id,
  toDate('2019-01-01') as date_day
"""

custom_constraint_model_schema_yml = """
version: 2
models:
  - name: custom_column_constraint_model
    materialized: table
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: Int32
        codec: ZSTD
      - name: ts
        data_type: timestamp
      - name: col_ttl
        data_type: String
        ttl: ts + INTERVAL 1 DAY
"""

check_custom_constraints_model_sql = """
{{
  config(
    materialized = "table",
  )
}}

select
  101::Int32 as id,
  timestamp('2025-04-16') as ts,
  'blue' as col_ttl
"""
