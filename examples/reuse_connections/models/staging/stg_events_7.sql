{{ config(materialized='table') }}

select
    event_id,
    payload,
    category,
    amount,
    created_at,
    7 as source_idx
from {{ ref('events') }}
