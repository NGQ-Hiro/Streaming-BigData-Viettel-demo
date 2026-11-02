{{ config(
    incremental_strategy='merge',
    unique_key='locationKey',
    options={'type': 'cow', 'primaryKey': 'locationKey', 'preCombineField': 'processed_time'}
) }}

WITH events AS (
  -- Collapse the raw events to one row per place first, so the state join and
  -- the surrogate key run on distinct locations, not on every event.
  SELECT city, state, lat, lon, max(processed_time) AS processed_time
  FROM {{ source('staging', 'listen_events') }}
  {% if is_incremental() %}
    -- Same result as filtering after the max(): a group's max is new iff any row is new.
    WHERE processed_time > (SELECT max(processed_time) FROM {{ this }})
  {% endif %}
  GROUP BY city, state, lat, lon
),
cte AS (
  SELECT
      {{ dbt_utils.generate_surrogate_key(['city', 'stateName', 'lat', 'lon']) }} as locationKey,
      city,
      COALESCE(state_codes.stateCode, 'NA') as stateCode,
      COALESCE(state_codes.stateName, 'NA') as stateName,
      lat as latitude,
      lon as longitude,
      max(processed_time) as processed_time
  FROM events
  LEFT JOIN {{ ref('state_codes') }} on events.state = state_codes.stateCode
  -- Second group-by keeps the old behavior when two raw states both map to 'NA'.
  GROUP BY city, stateName, stateCode, lat, lon
)

select * from cte
