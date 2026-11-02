{{ config(
    incremental_strategy='merge',
    unique_key='event_key',
    options={'type': 'cow', 'primaryKey': 'event_key', 'preCombineField': 'processed_time'}
) }}

WITH events AS (
    -- Normalize once: each expression was repeated across the joins below.
    SELECT
        {{ dbt_utils.generate_surrogate_key(['userId', 'ts', 'artist', 'song']) }} AS event_key,
        processed_time,
        ts,
        userId,
        city,
        state,
        lon,
        lat,
        date_trunc('HOUR', timestamp_millis(ts)) AS event_hour,
        CAST(timestamp_millis(ts) AS DATE) AS event_date,
        REPLACE(REPLACE(artist, '"', ''), '\\', '') AS artist_clean,
        REPLACE(REPLACE(song, '"', ''), '\\', '') AS song_clean
    FROM {{ source('staging', 'listen_events') }}
    {% if is_incremental() %}
    WHERE processed_time > (SELECT max(processed_time) FROM {{ this }})
    {% endif %}
)

-- Small dims are broadcast, so the big events table is not shuffled for them.
-- shortcut: dim_users stays a shuffle join (1M users); revisit if dims grow past memory.
SELECT /*+ BROADCAST(dim_datetime), BROADCAST(dim_locations), BROADCAST(dim_artists), BROADCAST(dim_songs) */
    e.event_key,
    e.processed_time,
    dateKey,
    userKey,
    locationKey,
    artistKey,
    songId AS songKey,
    e.ts AS ts
FROM events e
LEFT JOIN {{ ref('dim_datetime') }}
    ON dim_datetime.datehour = e.event_hour
LEFT JOIN {{ ref('dim_users') }}
    ON e.userId = dim_users.userId AND e.event_date >= dim_users.rowActivationDate AND e.event_date < dim_users.rowExpirationDate
LEFT JOIN {{ ref('dim_locations') }}
    ON e.city = dim_locations.city AND e.state = dim_locations.stateCode AND e.lon = dim_locations.longitude AND e.lat = dim_locations.latitude
LEFT JOIN {{ ref('dim_artists') }}
    ON e.artist_clean = dim_artists.artistName
LEFT JOIN {{ ref('dim_songs') }}
    ON e.artist_clean = dim_songs.artistName AND e.song_clean = dim_songs.title
