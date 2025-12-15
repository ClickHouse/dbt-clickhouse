{{
  config(
    materialized='materialized_view',
    engine='SummingMergeTree()',
    order_by='(user_id)'
  )
}}

--mobile:begin
SELECT
    user_id,
    count() AS total_events,
    sum(value) AS total_value,
    uniqExact(event_type) AS unique_event_types
FROM {{ ref('events') }}
WHERE event_type LIKE 'mobile.%'
GROUP BY user_id
--mobile:end


--web:begin
SELECT
    user_id,
    count() AS total_events,
    sum(value) AS total_value,
    uniqExact(event_type) AS unique_event_types
FROM {{ ref('events') }}
WHERE event_type NOT LIKE 'mobile.%'
GROUP BY user_id
--web:end

