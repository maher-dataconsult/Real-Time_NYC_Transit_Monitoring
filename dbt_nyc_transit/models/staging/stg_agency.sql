with source as (
    select * from {{ source('mta_raw', 'AGENCY') }}
)

select
    agency_id,
    agency_name,
    agency_url,
    agency_timezone,
    agency_lang,
    agency_phone
from source