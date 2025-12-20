with source as (
    select * from {{ source('mta_raw', 'STOPS') }}
)

select
    stop_id,
    stop_name,
    stop_lat,
    stop_lon,
    location_type::integer as location_type, 
    parent_station
from source