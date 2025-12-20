with source as (
    select * from {{ source('mta_raw', 'TRANSFERS') }}
)

select
    from_stop_id,
    to_stop_id,
    transfer_type::integer as transfer_type,
    min_transfer_time::integer as min_transfer_time
from source