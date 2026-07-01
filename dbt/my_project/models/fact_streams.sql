SELECT
    dateKey,
    userKey,
    locationKey,
    artistKey,
    songId as songKey,
    listen_events.ts AS ts
FROM {{source('staging', 'listen_events') }} 
LEFT JOIN {{ref('dim_datetime')}} 
    ON dim_datetime.datehour = date_trunc(hour, listen_events.ts)
LEFT JOIN {{ref('dim_users')}} 
    ON listen_events.userId = dim_users.userId AND CAST(listen_events.ts as DATE) >= dim_users.rowActivationDate AND CAST(listen_events.ts as DATE) < dim_users.rowExpirationDate
LEFT JOIN {{ref('dim_locations')}} 
    ON listen_events.city = dim_locations.city AND listen_events.state = dim_locations.stateCode AND listen_events.lon = dim_locations.longitude AND listen_events.lat = dim_locations.latitude
LEFT JOIN {{ref('dim_artists')}} 
    ON REPLACE(REPLACE(listen_events.artist, '"', ''), '\\', '') = dim_artists.artistName
LEFT JOIN {{ref('dim_songs')}} 
    ON REPLACE(REPLACE(listen_events.artist, '"', ''), '\\', '') = dim_songs.artistName AND REPLACE(REPLACE(listen_events.song, '"', ''), '\\', '') = dim_songs.title