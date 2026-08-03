{{ config(materialized='table') }}

select category, sum(amount) as total_amount, count() as cnt, 5 as mart_idx
from (
    select * from {{ ref('stg_events_5') }}
    union all
    select * from {{ ref('stg_events_6') }}
)
group by category
