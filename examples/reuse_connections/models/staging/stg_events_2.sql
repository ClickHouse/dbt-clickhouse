{{ config(materialized='table') }}

select
    event_id,
    payload,
    category,
    amount,
    created_at,
    2 as source_idx
from {{ ref('events') }}
