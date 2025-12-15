{{ config(materialized='materialized_view', catchup=True) }}

{{ materialization_target_table(ref("new_mv_target_model")) }}

SELECT
    toDate(event_time) AS event_date,
    event_type,
    count() AS event_count,
    sum(value) AS total_value
FROM {{ ref('events') }}
WHERE event_type not LIKE 'mobile.%'
GROUP BY event_date, event_type

