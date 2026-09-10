

with date_series as (
    select
        explode(sequence(
            to_timestamp('2025-01-01 00:00:00'),
            to_timestamp('2025-01-01 00:00:00') + interval 37513 hours,
            interval 1 hour
        )) as date   -- 37514 hourly rows total (37513 + start)
)
select
    date_format(date, 'yyyyMMddHH') as dateKey,
    date as dateHour,
    hour(date) as hour,
    dayofweek(date) as dayOfWeek,
    dayofmonth(date) as dayOfMonth,
    weekofyear(date) as weekOfYear,
    month(date) as month,
    year(date) as year,
    -- Spark's dayofweek() returns 1=Sunday..7=Saturday, so (1,7) is still weekend
    case when dayofweek(date) in (1,7) then true else false end as weekendFlag
from date_series
