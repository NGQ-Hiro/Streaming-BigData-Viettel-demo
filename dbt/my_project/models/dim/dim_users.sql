{{ config(
    incremental_strategy='merge',
    unique_key='userKey',
    options={'type': 'cow', 'primaryKey': 'userKey', 'preCombineField': 'processed_time'}
) }}

WITH user_pt AS (
    -- Last event time per user; a user is "affected" when it is newer than the table's max.
    SELECT CAST(userId AS BIGINT) AS uid, max(processed_time) AS processed_time
    FROM {{ source('staging', 'listen_events') }}
    GROUP BY CAST(userId AS BIGINT)
)

SELECT
 
    {{ dbt_utils.generate_surrogate_key(['userId', 'rowActivationDate', 'level']) }} as userKey,
    d.*,
    user_pt.processed_time AS processed_time
FROM
(
    SELECT 
        CAST(userId AS BIGINT) as userId, firstName, lastName, gender, level, CAST(registration as BIGINT) as registration, minDate as rowActivationDate,
        -- Choose the start date from the next record and add that as the expiration date for the current record
        LEAD(minDate, 1, '9999-12-31') OVER(PARTITION BY userId, firstName, lastName, gender ORDER BY grouped) as rowExpirationDate,
        -- Assign a flag indicating current user level (paid or free)
        CASE WHEN RANK() OVER(PARTITION BY userId, firstName, lastName, gender ORDER BY grouped desc) = 1 THEN 1 ELSE 0 END AS currentRow
    FROM
    (
        -- Find the earliest date available for each free/paid status change
        SELECT userId, firstName, lastName, gender, registration, level, grouped, cast(timestamp_millis(min(date)) as date) as minDate
        FROM
        -- Create distinct group of each level change to identify the change in level accurately
        (
            SELECT 
                *, 
                SUM(lagged) OVER(PARTITION BY userId, firstName, lastName, gender ORDER BY date) as grouped
            FROM
            -- Lag the level and see where the user changes level from free to paid or otherwise
            (
                SELECT 
                    *, 
                    CASE WHEN LAG(level, 1, 'NA') OVER(PARTITION BY userId, firstName, lastName, gender ORDER BY date) <> level THEN 1 ELSE 0 END AS lagged
                from
                -- Select distinct state of user in each timestamp
                (
                    SELECT  distinct 
                        userId
                        ,firstName
                        ,lastName
                        ,gender
                        ,registration
                        ,level
                        ,ts AS date
                    FROM {{ source('staging', 'listen_events') }}
                    {% if is_incremental() %}
                        -- SCD2 needs a user's whole history: rebuild only users with new events, all their rows.
                        where cast(userId as bigint) in (
                            select uid from user_pt where processed_time > (select max(processed_time) from {{ this }})
                        )
                    {% endif%}
                )
            )
        )
        GROUP BY userId, firstName, lastName, gender, registration, level, grouped
    )
) d
JOIN user_pt ON d.userId = user_pt.uid
