{{ config(materialized='table') }}

select category, sum(amount) as total_amount, count() as cnt, 1 as mart_idx
from (
    select * from {{ ref('stg_events_1') }}
    union all
    select * from {{ ref('stg_events_2') }}
)
group by category
