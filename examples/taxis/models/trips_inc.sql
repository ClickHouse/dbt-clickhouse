with (select start, end from {{ ref('trips_rand') }} ORDER BY date_time DESC LIMIT 1) as range
select rand() as trip_id, * EXCEPT trip_id from {{ source('taxis_source', 'trips') }} where bitAnd(trip_id, 1023) between range.1 and range.2
