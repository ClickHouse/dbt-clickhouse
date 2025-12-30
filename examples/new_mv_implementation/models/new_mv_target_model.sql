{{
  config(
    materialized='table',
    engine='SummingMergeTree()',
    order_by='(event_date, event_type)',
    partition_by='toYYYYMM(event_date)',
    on_schema_change='sync_all_columns'
  )
}}


SELECT
    toDate(now()) AS event_date,
    '' AS event_type,
    toUInt64(0) AS event_count,
    toFloat64(0) AS total_value,
    toUInt64(0) AS the_new_column
WHERE 0  -- Creates empty table with correct schema

