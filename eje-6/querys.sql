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