WITH cte AS (
  SELECT 
      {{ dbt_utils.generate_surrogate_key(['city', 'stateName', 'lat', 'lon']) }} as locationKey,
      city,
      COALESCE(state_codes.stateCode, 'NA') as stateCode,
      COALESCE(state_codes.stateName, 'NA') as stateName,
      lat as latitude,
      lon as longitude,
      max(processed_time) as processed_time
  FROM {{ source('staging', 'listen_events') }}
  LEFT JOIN {{ ref('state_codes') }} on listen_events.state = state_codes.stateCode
  GROUP BY city, stateName, stateCode, lat, lon
)

select * from cte
{% if is_incremental() %}
  where processed_time > (select max(processed_time) from {{ this }})
{% endif %}