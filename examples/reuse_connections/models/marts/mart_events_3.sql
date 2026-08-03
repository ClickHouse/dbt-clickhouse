{{ config(materialized='table') }}

select category, sum(amount) as total_amount, count() as cnt, 3 as mart_idx
from (
    select * from {{ ref('stg_events_3') }}
    union all
    select * from {{ ref('stg_events_4') }}
)
group by category
