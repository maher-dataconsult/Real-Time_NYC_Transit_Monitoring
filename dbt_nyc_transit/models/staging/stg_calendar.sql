with source as (
    select * from {{ source('mta_raw', 'CALENDAR') }}
)

select
    service_id,
    -- Convert 0/1 to Boolean
    case when monday = 1 then true else false end as is_monday,
    case when tuesday = 1 then true else false end as is_tuesday,
    case when wednesday = 1 then true else false end as is_wednesday,
    case when thursday = 1 then true else false end as is_thursday,
    case when friday = 1 then true else false end as is_friday,
    case when saturday = 1 then true else false end as is_saturday,
    case when sunday = 1 then true else false end as is_sunday,
    -- Parse Dates
    try_to_date(start_date::varchar, 'YYYYMMDD') as start_date,
    try_to_date(end_date::varchar, 'YYYYMMDD') as end_date
from source