import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist text,
    auth text,
    first_name text,
    gender text,
    item_in_session int,
    last_name text,
    length float,
    level text,
    location text,
    method text,
    page text,
    registration float,
    session_id int,
    song text,
    status int,
    ts timestamp,
    user_agent text,
    user_id int)  
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs int,
    artist_id text,
    artist_latitude float,
    artist_longitude float,
    artist_location text,
    artist_name text,
    song_id text,
    title text,
    duration float,
    year int)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id int IDENTITY(0,1), 
    start_time timestamp NOT NULL,
    user_id int NOT NULL, 
    level text, 
    song_id text, 
    artist_id text, 
    session_id int, 
    location text, 
    user_agent text,
    PRIMARY KEY(songplay_id))
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id int NOT NULL, 
    first_name text NOT NULL, 
    last_name text NOT NULL, 
    gender text, 
    level text,
    PRIMARY KEY(user_id))
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id text NOT NULL, 
    title text NOT NULL, 
    artist_id text NOT NULL, 
    year int NOT NULL, 
    duration numeric,
    PRIMARY KEY(song_id))
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id text NOT NULL, 
    name text NOT NULL, 
    location text, 
    latitude numeric, 
    longitude numeric,
    PRIMARY KEY(artist_id))
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time timestamp NOT NULL, 
    hour int NOT NULL, 
    day int NOT NULL, 
    week int NOT NULL, 
    month int NOT NULL,
    year int NOT NULL, 
    weekday text NOT NULL,
    PRIMARY KEY(start_time))
""")

# STAGING TABLES

# Refer to https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-conversion.html#copy-timeformat as TIMEFORMAT documentation.
staging_events_copy = ("""
COPY staging_events
FROM {}
IAM_ROLE {}
FORMAT JSON AS {}
TIMEFORMAT 'epochmillisecs';
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

# JSON 'auto' - automatically load fields from JSON file - Refer to https://docs.aws.amazon.com/redshift/latest/dg/copy-usage_notes-copy-from-json.html
staging_songs_copy = ("""
COPY staging_songs
FROM {}
IAM_ROLE {}
JSON 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location,  user_agent)
SELECT e.ts, e.user_id, e.level, s.song_id, s.artist_id, e.session_id, e.location, e.user_agent            
FROM staging_events e
JOIN staging_songs s ON e.artist = s.artist_name AND e.song = s.title
WHERE e.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT user_id, first_name, last_name, gender, level
FROM staging_events
WHERE page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT ts, date_part(hour, ts), date_part(day, ts), date_part(week, ts), date_part(month, ts), date_part(year, ts), date_part(dow, ts)
FROM staging_events 
WHERE page = 'NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
