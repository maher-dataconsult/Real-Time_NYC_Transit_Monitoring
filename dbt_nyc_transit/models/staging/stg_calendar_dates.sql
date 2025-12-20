with source as (
    select * from {{ source('mta_raw', 'CALENDAR_DATES') }}
)

select
    service_id::text as service_id,
    try_to_date(date::varchar, 'YYYYMMDD') as date,
    exception_type::integer as exception_type
from source