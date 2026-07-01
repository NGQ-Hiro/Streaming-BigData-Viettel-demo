SELECT 
    {{dbt_utils.generate_surrogate_key(['cleaned_artist_name'])}} as artistKey,
    MAX(artist_latitude) AS latitude,
    MAX(artist_longitude) AS longitude,
    MAX(artist_location) AS location,
    cleaned_artist_name AS artistName
FROM (
    SELECT *,
        COALESCE(REPLACE(REPLACE(artist_name, '"', ''), '\\', ''), '') AS cleaned_artist_name
    FROM {{ source('staging', 'songs') }}
) t
GROUP BY cleaned_artist_name
