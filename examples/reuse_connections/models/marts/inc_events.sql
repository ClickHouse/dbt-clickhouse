{{ config(materialized='incremental', incremental_strategy='delete_insert', unique_key='category', use_lw_deletes=true) }}

select category, sum(amount) as total_amount, count() as cnt
from {{ ref('events') }}
{% if is_incremental() %}
where created_at > (select max(created_at) from {{ this }})
{% endif %}
group by category
