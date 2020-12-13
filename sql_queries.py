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
CREATE TABLE {} (
    songplay_id int,
    start_time time,
    user_id int,
    level text,
    song_id text,
    artist_id text,
    session_id int,
    location text,
    user_agent text
);
""".format(
    SONGS_PLAY_TABLE
)

user_table_create = """
CREATE TABLE {} (
    user_id int,
    first_name text,
    last_name text,
    gender boolean,
    level text
);
""".format(
    USERS_TABLE
)

song_table_create = """
CREATE TABLE {} (
    song_id text,
    title text,
    artist_id text,
    year int,
    duration real
);
""".format(
    SONGS_TABLE
)

artist_table_create = """
CREATE TABLE {} (
    artist_id text,
    name text,
    location text,
    latitude text,
    longitude text
);
""".format(
    ARTISTS_TABLE
)

time_table_create = """
CREATE TABLE {} (
    start_time time,
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday boolean
)
""".format(
    TIME_TABLE
)

# INSERT RECORDS

songplay_table_insert = """
INSERT INTO {} (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)
""".format(
    SONGS_PLAY_TABLE
)

user_table_insert = """
INSERT INTO {} (user_id, first_name, last_name, gender, level) 
VALUES(%s, %s, %s, %s, %s)
""".format(
    USERS_TABLE
)

song_table_insert = """
INSERT INTO {} (song_id, title, artist_id, year, duration) 
VALUES(%s, %s, %s, %s, %s)
""".format(
    SONGS_TABLE
)

artist_table_insert = """
INSERT INTO {} (artist_id, name, location, latitude, longitude) 
VALUES(%s, %s, %s, %s, %s))
""".format(
    ARTISTS_TABLE
)


time_table_insert = """
INSERT INTO {} (start_time, hour, day, week, month, year, weekday) 
VALUES(%s, %s, %s, %s, %s), %s, %s)
""".format(
    TIME_TABLE
)

# FIND SONGS

song_select = """
"""

# QUERY LISTS

create_table_queries = [
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
]
drop_table_queries = [
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
