with source as (
    select * from {{ source('mta_raw', 'ROUTES') }}
)

select
    route_id,
    agency_id,
    route_short_name,
    route_long_name,
    route_desc,
    route_type::integer as route_type,
    route_url,
    route_color,
    route_text_color
from source