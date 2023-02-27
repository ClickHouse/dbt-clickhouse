with (select start, end from {{ ref('trips_rand') }} ORDER BY date_time DESC LIMIT 1) as range,
(select count() from {{ ref('trips_rand') }}) as run_num

select rand() as rand_trip_id, * EXCEPT trip_id, run_num, trip_id as orig_id from {{ source('taxis_source', 'trips') }}
  LEFT JOIN numbers(24) as sysnum ON 1 = 1
  where bitAnd(orig_id, 1023) between range.1 and range.2


