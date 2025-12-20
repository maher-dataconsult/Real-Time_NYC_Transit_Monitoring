with source as (
    select * from {{ source('mta_raw', 'SHAPES') }}
)

select
    shape_id,
    shape_pt_lat,
    shape_pt_lon,
    shape_pt_sequence::integer as shape_pt_sequence
from source