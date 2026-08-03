{{ config(materialized='materialized_view') }}

select category, sum(total_amount) as grand_total, sum(cnt) as grand_cnt
from {{ ref('mart_events_3') }}
group by category
