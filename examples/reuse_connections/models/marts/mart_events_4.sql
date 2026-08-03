{{ config(materialized='table') }}

select category, sum(amount) as total_amount, count() as cnt, 4 as mart_idx
from (
    select * from {{ ref('stg_events_4') }}
    union all
    select * from {{ ref('stg_events_5') }}
)
group by category
