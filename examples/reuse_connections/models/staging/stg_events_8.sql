{{ config(materialized='table') }}

select
    event_id,
    payload,
    category,
    amount,
    created_at,
    8 as source_idx
from {{ ref('events') }}
