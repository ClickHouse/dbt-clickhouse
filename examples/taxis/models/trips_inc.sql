select
    rand() as rand_trip_id,
    *,
    trip_id as orig_id
from {{ source('taxis_source', 'trips') }}
