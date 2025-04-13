column_ttl_model_yml = """
version: 2
models:
  - name: column_ttl_model
    config:
      contract:
        enforced: true
    columns:
      - name: datetime_col
        data_type: DateTime
      - name: id
        data_type: UInt32
        ttl: toStartOfDay(datetime_col) + INTERVAL 1 DAY
"""


column_ttl_model_sql = """
{{
  config(
    materialized = "table"
  )
}}

select
  now() as datetime_col,
  1::UInt32 as id
"""