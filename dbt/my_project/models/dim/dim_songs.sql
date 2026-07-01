
SELECT 
    MIN(song_id) AS songId,  
    cleaned_artist_name AS artistName,
    cleaned_title AS title,
    MAX(duration) AS duration,
    MAX(key) AS key,
    MAX(key_confidence) AS keyConfidence,
    MAX(loudness) AS loudness,
    MAX(song_hotttnesss) AS songHotness,
    MAX(tempo) AS tempo,
    MAX(year) AS year
FROM (
    SELECT *,
        COALESCE(REPLACE(REPLACE(artist_name, '"', ''), '\\', ''), '') AS cleaned_artist_name,
        COALESCE(REPLACE(REPLACE(title, '"', ''), '\\', ''), '') AS cleaned_title
    FROM {{ source('staging', 'songs') }}
) t
GROUP BY cleaned_artist_name, cleaned_title

