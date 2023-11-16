model_schema_yml = """
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
        data_type: Int32
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
  1 as id,
  '2019-01-01' as date_day
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
