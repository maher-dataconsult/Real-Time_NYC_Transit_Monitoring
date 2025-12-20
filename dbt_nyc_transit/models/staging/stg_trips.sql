with source as (
    select * from {{ source('mta_raw', 'TRIPS') }}
)

select
    trip_id,
    route_id,
    service_id,
    trip_headsign,
    direction_id::integer as direction_id,
    shape_id
from source