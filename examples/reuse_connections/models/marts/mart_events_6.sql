{{ config(materialized='table') }}

select category, sum(amount) as total_amount, count() as cnt, 6 as mart_idx
from (
    select * from {{ ref('stg_events_6') }}
    union all
    select * from {{ ref('stg_events_7') }}
)
group by category
