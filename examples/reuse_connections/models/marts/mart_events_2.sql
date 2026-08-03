{{ config(materialized='table') }}

select category, sum(amount) as total_amount, count() as cnt, 2 as mart_idx
from (
    select * from {{ ref('stg_events_2') }}
    union all
    select * from {{ ref('stg_events_3') }}
)
group by category
