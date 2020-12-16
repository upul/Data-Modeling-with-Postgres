# DROP TABLES

USERS_TABLE = "users"
SONGS_TABLE = "songs"
ARTISTS_TABLE = "artists"
TIME_TABLE = "time"
SONGS_PLAY_TABLE = "songplays"

songplay_table_drop = f"DROP TABLE IF EXISTS {SONGS_PLAY_TABLE};"
user_table_drop = f"DROP TABLE IF EXISTS {USERS_TABLE};"
song_table_drop = f"DROP TABLE IF EXISTS {SONGS_TABLE};"
artist_table_drop = f"DROP TABLE IF EXISTS {ARTISTS_TABLE};"
time_table_drop = f"DROP TABLE IF EXISTS {TIME_TABLE};"

# CREATE TABLES

songplay_table_create = """
CREATE TABLE IF NOT EXISTS {} (
    songplay_id SERIAL NOT NULL,
    start_time bigint NOT NULL,
    user_id int NOT NULL,
    level text,
    song_id text,
    artist_id text,
    session_id int,
    location text,
    user_agent text,
    
    PRIMARY KEY(songplay_id),
    FOREIGN KEY(user_id) REFERENCES {}(user_id),
    FOREIGN KEY(song_id) REFERENCES {}(song_id),
    FOREIGN KEY(artist_id) REFERENCES {}(artist_id),
    FOREIGN KEY(start_time) REFERENCES {}(start_time)

    
);
""".format(
    SONGS_PLAY_TABLE, USERS_TABLE, SONGS_TABLE, ARTISTS_TABLE, TIME_TABLE
)


user_table_create = """
CREATE TABLE IF NOT EXISTS {} (
    user_id int NOT NULL,
    first_name text,
    last_name text,
    gender text,
    level text,

    PRIMARY KEY(user_id)
);
""".format(
    USERS_TABLE
)

song_table_create = """
CREATE TABLE IF NOT EXISTS {} (
    song_id text NOT NULL,
    title text,
    artist_id text,
    year int,
    duration real,

    PRIMARY KEY(song_id)
);
""".format(
    SONGS_TABLE
)

artist_table_create = """
CREATE TABLE IF NOT EXISTS {} (
    artist_id text NOT NULL,
    name text,
    location text,
    latitude text,
    longitude text,

    PRIMARY KEY(artist_id)
);
""".format(
    ARTISTS_TABLE
)

time_table_create = """
CREATE TABLE IF NOT EXISTS {} (
    start_time bigint NOT NULL,
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday text,

    PRIMARY KEY(start_time)
)
""".format(
    TIME_TABLE
)

# INSERT RECORDS

songplay_table_insert = """
INSERT INTO {} (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
VALUES(%s, %s, %s, %s, %s, %s, %s, %s) 
ON CONFLICT(songplay_id) DO NOTHING;
""".format(
    SONGS_PLAY_TABLE
)

user_table_insert = """
INSERT INTO {} (user_id, first_name, last_name, gender, level) 
VALUES(%s, %s, %s, %s, %s)
ON CONFLICT(user_id) DO UPDATE SET level = excluded.level
""".format(
    USERS_TABLE
)

song_table_insert = """
INSERT INTO {} (song_id, title, artist_id, year, duration) 
VALUES(%s, %s, %s, %s, %s)
ON CONFLICT(song_id) DO NOTHING;
""".format(
    SONGS_TABLE
)

artist_table_insert = """
INSERT INTO {} (artist_id, name, location, latitude, longitude) 
VALUES(%s, %s, %s, %s, %s)
ON CONFLICT(artist_id) DO NOTHING;
""".format(
    ARTISTS_TABLE
)


time_table_insert = """
INSERT INTO {} (start_time, hour, day, week, month, year, weekday) 
VALUES(%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT(start_time) DO NOTHING;
""".format(
    TIME_TABLE
)

# FIND SONGS

song_select = """

SELECT songs.song_id, artists.artist_id 
FROM songs JOIN artists ON songs.artist_id = artists.artist_id
WHERE songs.title = %s AND artists.name = %s AND songs.duration = %s;
"""

# QUERY LISTS

create_table_queries = [
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
    songplay_table_create,
]
drop_table_queries = [
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
    songplay_table_drop,
]
