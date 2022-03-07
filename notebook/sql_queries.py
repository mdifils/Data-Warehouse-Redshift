import configparser

##################### LOAD PARAMS FROM CONFIG FILE ###################

config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config["S3"]["LOG_DATA"]
SONG_DATA = config["S3"]["SONG_DATA"]
LOG_JSONPATH = config["S3"]["LOG_JSONPATH"]
DWH_ROLE_ARN = config["IAM_ROLE"]["ARN"]

###################### DROP TABLES ###################################

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

##################### CREATE TABLES ##################################

staging_events_table_create = """
CREATE TABLE IF NOT EXISTS staging_events 
(
    artist TEXT,
    auth TEXT,
    first_name TEXT,
    gender CHAR,
    item_in_session INTEGER,
    last_name TEXT,
    length FLOAT,
    level TEXT,
    location TEXT,
    method TEXT,
    page TEXT,
    registration BIGINT,
    session_id INTEGER,
    song TEXT,
    status INTEGER,
    ts TIMESTAMP,
    user_agent TEXT,
    user_id INTEGER
)
"""

staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS staging_songs 
(
    num_songs INTEGER,
    artist_id TEXT,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location TEXT,
    artist_name TEXT,
    song_id TEXT,
    title TEXT,
    duration FLOAT,
    year INTEGER
)
"""

songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplays 
(
    songplay_id INTEGER IDENTITY(1,1) PRIMARY KEY,
    start_time TIMESTAMP REFERENCES time,
    user_id INTEGER REFERENCES users,
    level TEXT,
    song_id TEXT REFERENCES songs,
    artist_id TEXT REFERENCES artists,
    session_id INTEGER,
    location TEXT,
    user_agent TEXT
)
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    gender CHAR,
    level TEXT
)
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS songs (
    song_id TEXT PRIMARY KEY,
    title TEXT,
    artist_id TEXT,
    year INTEGER,
    duration FLOAT
)
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS artists (
    artist_id TEXT PRIMARY KEY,
    name TEXT,
    location TEXT,
    lattitude FLOAT,
    longitude FLOAT
)
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER
)
"""

################### COPY FROM S3 TO STAGING TABLES ####################

staging_events_copy = """
COPY staging_events 
FROM {}
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2' 
TIMEFORMAT as 'epochmillisecs'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
COMPUPDATE OFF
FORMAT AS JSON {};
""".format(LOG_DATA, DWH_ROLE_ARN, LOG_JSONPATH)

staging_songs_copy = """
COPY staging_songs 
FROM {}
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
COMPUPDATE OFF
FORMAT AS JSON 'auto';
""".format(SONG_DATA, DWH_ROLE_ARN)

##################### INSERT INTO TABLES ##############################

song_table_insert = """
INSERT INTO songs 
(
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs
    WHERE song_id IS NOT NULL
);
"""

artist_table_insert = """
INSERT INTO artists 
(
    SELECT DISTINCT
        artist_id,
        artist_name AS name,
        artist_location AS location,
        artist_latitude AS lattitude,
        artist_longitude AS longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL
);
"""

user_table_insert = """
INSERT INTO users 
(
    SELECT DISTINCT user_id, first_name, last_name, gender, level
    FROM staging_events
    WHERE page = 'NextSong'
    AND user_id IS NOT NULL
);
"""

time_table_insert = """
INSERT INTO time 
(
    SELECT DISTINCT
        ts AS start_time,
        EXTRACT (hour FROM ts) AS hour,
        EXTRACT (day FROM ts) AS day,
        EXTRACT (week FROM ts) AS week,
        EXTRACT (month FROM ts) AS month,
        EXTRACT (year FROM ts) AS year,
        EXTRACT (dayofweek FROM ts) AS weekday
    FROM staging_events
    WHERE page = 'NextSong'
    AND ts IS NOT NULL
);
"""

songplay_table_insert = """
INSERT INTO songplays 
(start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
SELECT
    se.ts AS start_time,
    se.user_id AS user_id,
    se.level AS level,
    ss.song_id AS song_id,
    ss.artist_id AS artist_id,
    se.session_id AS session_id,
    se.location AS location,
    se.user_agent AS user_agent
FROM staging_events se
JOIN staging_songs ss 
ON se.song = ss.title
AND se.artist = ss.artist_name
WHERE se.page = 'NextSong';
"""

#----------------------------------------- QUERY LISTS -------------------------

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    song_table_create,
    artist_table_create,
    user_table_create,
    time_table_create,
    songplay_table_create
]

drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    time_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop
]

copy_table_queries = [
    staging_events_copy, 
    staging_songs_copy
]

insert_table_queries = [ 
    song_table_insert, 
    artist_table_insert,
    user_table_insert,
    time_table_insert,
    songplay_table_insert
]