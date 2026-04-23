import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Text

# ==============================
# CONSTANTES
# ==============================
LOAD_DATA=False
LOAD_DATA_TABLES=False


# ==============================
# CONFIG
# ==============================
engine= create_engine("postgresql://postgres:@localhost:5432/data_spotify")

# ==============================
# LEER DATA
# ==============================

df = pd.read_csv("./data/data_spotify.csv")

# print(df.head())
# print("----------------------")
# print(df.info())
# print("----------------------")

# print(df.isnull().sum())
# print("----------------------")

# print(df.duplicated().sum())
# print("----------------------")
# print(df.columns)

# 1. Unnamed: 0 → eliminar
# 2. 1 fila con nulls → decidir qué hacer
# 3. artists → potencial problema futuro

#ELIMINAR COLUMNA BASURA
df=df.drop(columns=['Unnamed: 0'])
# ELIMINAR FILA NULA
#df = df.dropna()
df = df.dropna(subset=["artists","track_name","album_name"])
# print(df.isnull().sum())
# print(df[df["artists"].isnull()]
# )
df = df.drop_duplicates(subset=["track_id"])

# print(df.info())
# ==============================
# CARGAR DATOS CRUDOS
# ==============================
if LOAD_DATA:
    df.to_sql("tracks", engine, if_exists="replace", index=False)
    print("datos carados a posgres")

# ==============================
# 1 EXTRACCION (READ)
# ==============================
spotify_raw = pd.read_sql("SELECT * FROM tracks",engine)

#eliminar espacios si los hay
text_cols=["artists","album_name","track_name","track_genre"]
for col in text_cols:
    spotify_raw[col]=spotify_raw[col].str.strip()
# print(spotify_raw.head())

# print(df["popularity"].min())
# print(df["popularity"].max())
# print(df["duration_ms"].min())
# print(df["duration_ms"].max())

# print(df["track_name"].value_counts().head())
# print(df["artists"].value_counts())

#Table genero
genre = spotify_raw[["track_genre"]].drop_duplicates().reset_index(drop=True)
genre["genre_id"]=genre.index + 1
genre=genre[["genre_id","track_genre"]]
spotify_raw = spotify_raw.merge(genre, on=["track_genre"], how="left")
# print(genre.head())
# print(spotify_raw.head())

#table album
# album = spotify_raw[["album_name"]].drop_duplicates().reset_index(drop=True)
# album["album_id"]=album.index + 1
# album=album[["album_id","album_name"]]
# spotify_raw = spotify_raw.merge(album, on=["album_name"], how="left")

# print(album)
# print(spotify_raw.head())

#table track
track = spotify_raw[["track_id","album_name","track_name","popularity","duration_ms","explicit","danceability","energy","key","loudness","mode","speechiness","acousticness","instrumentalness","liveness","valence","tempo","time_signature","genre_id"]]

print(track.head())
print(track.duplicated().sum())

# TABLE ARTIST
#problema con artists, cuando hay un colaborador en la cancion , hay que separar los nombres ["Drake", "Future"]

spotify_raw["artists"]=spotify_raw["artists"].str.split(";")

# #track_id | artists
# A1       | ["Drake", "Future"]
#despues de hacer explode
# track_id | artists
# A1       | Drake
# A1       | Future

df_artists=spotify_raw.explode("artists")
df_artists["artists"] = df_artists["artists"].str.strip()

artists = df_artists[["artists"]].drop_duplicates().reset_index(drop=True)
artists["artist_id"]= artists.index + 1
artists= artists[["artist_id","artists"]]

# print(artists.head())
# print(spotify_raw.head())

#Table TRACKS_ARTISTS - relacion muchos a muchos - tabla intermedia
#solamente hacemos el merge con df_artists y artists porque ya contienen el id del track
tracks_artists= df_artists.merge(artists, on="artists", how="left")
tracks_artists=tracks_artists[["track_id","artist_id"]]
# print(tracks_artists.info())
# print(track.info())

# print(track.shape)

# print(artists.shape)

# print(tracks_artists.shape)
# print(track.info())
# ==============================
# CARGAR DATOS
# ==============================
if LOAD_DATA_TABLES:
    # genre.to_sql("genre", engine, if_exists="append", index=False)
    # artists.to_sql("artists", engine, if_exists="append", index=False)
    track.to_sql("track", engine, if_exists="append", index=False)
    tracks_artists.to_sql("tracks_artists", engine, if_exists="append", index=False)
    print("datos carados a posgres")
