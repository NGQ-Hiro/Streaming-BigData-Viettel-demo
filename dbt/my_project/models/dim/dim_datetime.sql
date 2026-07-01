

with params as (
    select 
        to_timestamp('2025-01-01 00:00:00') as start_ts
),
date_series as (
    select 
        dateadd(hour, seq4(), p.start_ts) as date
    from params p,
         table(generator(rowcount => 37514))   -- total_hours + 1
)
select
    to_char(date, 'YYYYMMDDHH24') as dateKey,
    date as dateHour,
    hour(date) as hour,
    dayofweek(date) as dayOfWeek,
    day(date) as dayOfMonth,
    week(date) as weekOfYear,
    month(date) as month,
    year(date) as year,
    case when dayofweek(date) in (1,7) then true else false end as weekendFlag
from date_series