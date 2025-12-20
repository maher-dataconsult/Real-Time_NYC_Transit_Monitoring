with source as (
    select * from {{ source('mta_raw', 'STOP_TIMES') }}
)

select
    trip_id,
    stop_id,
    stop_sequence::integer as stop_sequence,
    arrival_time as arrival_time_text,
    departure_time as departure_time_text,
    
    -- 1- Transfer time to secomnds to can calculate the stop duration
    (split_part(arrival_time, ':', 1)::integer * 3600) + 
    (split_part(arrival_time, ':', 2)::integer * 60) + 
    (split_part(arrival_time, ':', 3)::integer) 
    as arrival_time_seconds,

    (split_part(departure_time, ':', 1)::integer * 3600) + 
    (split_part(departure_time, ':', 2)::integer * 60) + 
    (split_part(departure_time, ':', 3)::integer) 
    as departure_time_seconds,

    -- 2- Stop duration in seconds to can calculate the stop duration
    -- Subtract the departure time from the arrival time to know the stop duration

    ((split_part(departure_time, ':', 1)::integer * 3600) + 
     (split_part(departure_time, ':', 2)::integer * 60) + 
     (split_part(departure_time, ':', 3)::integer)) 
    - 
    ((split_part(arrival_time, ':', 1)::integer * 3600) + 
     (split_part(arrival_time, ':', 2)::integer * 60) + 
     (split_part(arrival_time, ':', 3)::integer)) 
    as stop_duration_seconds,

    -- 3- Check if the arrival time is the next day
    case 
        when split_part(arrival_time, ':', 1)::integer >= 24 then true 
        else false 
    end as is_next_day_arrival

from source