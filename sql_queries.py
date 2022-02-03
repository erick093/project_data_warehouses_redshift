import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = " DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = " DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = " DROP TABLE IF EXISTS songplays;"
user_table_drop = " DROP TABLE IF EXISTS users;"
song_table_drop = " DROP TABLE IF EXISTS songs;"
artist_table_drop = " DROP TABLE IF EXISTS artists;"
time_table_drop = " DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = (""" 
    CREATE TABLE IF NOT EXISTS staging_events (
            artist TEXT,
            auth VARCHAR(50),
            first_name VARCHAR(255),
            gender VARCHAR(50),
            item_in_session INTEGER,
            last_name VARCHAR(255),
            length REAL,
            level VARCHAR(50),
            location TEXT,
            method VARCHAR(50),
            page VARCHAR(50),
            registration DOUBLE PRECISION,
            session_id INTEGER,
            song TEXT,
            status INTEGER,
            ts BIGINT,
            user_agent TEXT,
            user_id INTEGER
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
            num_songs INTEGER,
            artist_id VARCHAR(50),
            artist_latitude REAL,
            artist_longitude REAL,
            artist_location TEXT,
            artist_name TEXT,
            song_id VARCHAR(50),
            title TEXT,
            duration REAL,
            year INTEGER
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
            songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
            start_time TIMESTAMP NOT NULL REFERENCES time(start_time),
            user_id INTEGER NOT NULL REFERENCES users(user_id),
            level VARCHAR(50),
            song_id VARCHAR(50) NOT NULL REFERENCES songs(song_id),
            artist_id VARCHAR(50) NOT NULL REFERENCES artists(artist_id),
            session_id INTEGER NOT NULL,
            location TEXT,
            user_agent TEXT
    );          
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            first_name VARCHAR(255),
            last_name VARCHAR(255),
            gender VARCHAR(50),
            level VARCHAR(50)
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
            song_id VARCHAR(50) PRIMARY KEY,
            title TEXT,
            artist_id VARCHAR(50) NOT NULL,
            year INTEGER,
            duration REAL
            );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
            artist_id VARCHAR(50) PRIMARY KEY,
            artist_name TEXT,
            artist_location TEXT,
            artist_latitude REAL,
            artist_longitude REAL
            );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
            start_time TIMESTAMP PRIMARY KEY,
            hour INTEGER,
            day INTEGER,
            week INTEGER,
            month INTEGER,
            year INTEGER,
            weekday INTEGER
            );
""")

# STAGING TABLES

staging_songs_copy = ("""
copy staging_songs
from {} 
iam_role {}
format as json 'auto' 
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

staging_events_copy = ("""
copy staging_events
from {}
iam_role {}
json {}
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent)
(SELECT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time, 
    events.user_id,
    events.level,
    songs.song_id,
    songs.artist_id,
    events.session_id,
    events.location,
    events.user_agent
FROM staging_events AS events
JOIN staging_songs AS songs
ON (events.artist = songs.artist_name) AND (events.song = songs.title)
AND (events.length = songs.duration)
WHERE events.page = 'NextSong')
""")

user_table_insert = ("""
INSERT INTO users (
    user_id,
    first_name,
    last_name,
    gender,
    level)
(SELECT events.user_id,
    events.first_name,
    events.last_name,
    events.gender,
    events.level
FROM staging_events events 
WHERE events.page = 'NextSong')
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
(SELECT song_id, title, artist_id, year, duration
FROM staging_songs)
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
(SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs)
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
(SELECT start_time,
    EXTRACT(hour FROM start_time) AS hour,
    EXTRACT(day FROM start_time) AS day,
    EXTRACT(week FROM start_time) AS week,
    EXTRACT(month FROM start_time) AS month,
    EXTRACT(year FROM start_time) AS year,
    EXTRACT(dow FROM start_time) AS weekday
FROM songplays)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
