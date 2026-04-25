create table tracks(
    track_id TEXT PRIMARY KEY,
    artists TEXT,
    album_name TEXT,
    track_name TEXT,
    popularity INT,
    duration_ms INT,
    explicit BOOLEAN,
    danceability FLOAT,
    energy FLOAT,
    key INT,
    loudness FLOAT,
    mode INT,
    speechiness FLOAT,
    acousticness FLOAT,
    instrumentalness FLOAT,
    liveness FLOAT,
    valence FLOAT,
    tempo FLOAT,
    time_signature INT,
    track_genre TEXT
);

CREATE TABLE genre(
    genre_id INT PRIMARY KEY,
    track_genre TEXT
);

CREATE TABLE artists(
    artist_id INT PRIMARY KEY,
    artists TEXT
);

CREATE TABLE track
(
    track_id TEXT PRIMARY KEY,
    album_name TEXT,
    track_name TEXT ,
    popularity INT,
    duration_ms INT,
    explicit BOOLEAN,
    danceability FLOAT,
    energy FLOAT,
    key INT,
    loudness FLOAT,
    mode INT,
    speechiness FLOAT,
    acousticness FLOAT,
    instrumentalness FLOAT,
    liveness FLOAT,
    valence FLOAT,
    tempo FLOAT,
    time_signature INT,
    genre_id INT,
    FOREIGN KEY (genre_id) REFERENCES genre (genre_id)
    
);

CREATE TABLE tracks_artists (
    track_id TEXT,
    artist_id INT,
    PRIMARY KEY (track_id, artist_id),
    FOREIGN KEY (track_id) REFERENCES track(track_id),
    FOREIGN KEY (artist_id) REFERENCES artists(artist_id)
);

-- TABLAS GOLD
    -- * Popularidad promedio por género
    SELECT  g.track_genre, 
    AVG(tr.popularity) as avg_popularity 
    FROM track tr
    JOIN genre g
    on tr.genre_id = g.genre_id
    GROUP BY g.track_genre
    ORDER BY avg_popularity DESC
    ;

    -- * Top artistas por popularidad
    SELECT
    ar.artists,ar.artist_id, AVG(tr.popularity) as artist_avg_popularirty
    FROM tracks_artists ta
    JOIN track tr
    ON ta.track_id = tr.track_id
    JOIN artists ar
    ON ta.artist_id = ar.artist_id
    GROUP BY ar.artists,ar.artist_id
    ORDER BY artist_avg_popularirty DESC;
    --LIMIT 10;
    -- * Duración promedio por género

    SELECT  g.track_genre,g.genre_id, 
    AVG(tr.duration_ms) as avg_duration/6000 as duration_minuts
    FROM track tr
    JOIN genre g
    on tr.genre_id = g.genre_id
    GROUP BY g.track_genre, g.genre_id
    ORDER BY avg_duration DESC
    LIMIT 10;

    -- * Canciones más energéticas o bailables
    SELECT tr.track_name,artists.artists, tr.danceability ,tr.energy, (tr.danceability + tr.energy) / 2 AS vibe_score
    FROM track tr
    JOIN tracks_artists ON tr.track_id = tracks_artists.track_id
    JOIN artists on artists.artist_id = tracks_artists.artist_id 
    ORDER BY vibe_score DESC
    LIMIt 10;

