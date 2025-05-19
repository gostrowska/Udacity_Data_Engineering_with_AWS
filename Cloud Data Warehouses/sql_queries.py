import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = """DROP TABLE IF EXISTS staging_events CASCADE;"""
staging_songs_table_drop = """DROP TABLE IF EXISTS staging_songs CASCADE;"""
songplay_table_drop = """DROP TABLE IF EXISTS songplays CASCADE;"""
user_table_drop = """DROP TABLE IF EXISTS users CASCADE;"""
song_table_drop = """DROP TABLE IF EXISTS songs CASCADE;"""
artist_table_drop = """DROP TABLE IF EXISTS artists CASCADE;"""
time_table_drop = """DROP TABLE IF EXISTS time CASCADE;"""

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(
    artist_name      TEXT,
    auth             TEXT,
    first_name       TEXT,
    gender           TEXT,
    item_in_session  INT,
    last_name        TEXT,
    length           FLOAT,
    level            TEXT,
    location         TEXT,
    method           TEXT,
    page             TEXT,
    registration     TEXT,
    sesion_id        INT,
    song             TEXT,
    status           INT,
    ts               BIGINT,
    user_agent       TEXT,
    user_id          TEXT
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
    num_songs        INT,
    artist_id        TEXT,
    artist_latitude  FLOAT,
    artist_longitude FLOAT,
    artist_location  TEXT,
    artist_name      TEXT,
    song_id          TEXT,
    title            TEXT,
    duration         FLOAT,
    year             INT
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
    songplay_id      BIGINT IDENTITY (0,1) PRIMARY KEY,
    start_time       TIMESTAMP NOT NULL sortkey,
    user_id          VARCHAR(20) NOT NULL,
    level            VARCHAR(4),
    song_id          VARCHAR(20) NOT NULL,
    artist_id        VARCHAR(20) NOT NULL distkey,
    session_id       INT,
    location         TEXT,
    user_agent       TEXT
)

""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_id         VARCHAR(20) PRIMARY KEY,
    first_name      VARCHAR(200),
    last_name       VARCHAR(200),
    gender          VARCHAR(10),
    level           VARCHAR(10)    
)
DISTKEY (user_id)

""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_id        VARCHAR(20) PRIMARY KEY,
    title          VARCHAR(2000),
    artist_id      VARCHAR(20) NOT NULL,
    year           INT4,
    duration       FLOAT
)
DISTKEY (song_id)
SORTKEY (artist_id)   

""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
    artist_id   VARCHAR(20) PRIMARY KEY,
    name        VARCHAR(2000),
    location    VARCHAR(200),
    latitude    FLOAT,
    longitude   FLOAT    
)
DISTKEY (artist_id)

""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    start_time  TIMESTAMP PRIMARY KEY,
    hour        INT4,
    day         INT4,
    week        INT4,
    month       INT4,
    year        INT4,
    weekday     INT4    
)
DISTKEY (start_time)
SORTKEY (start_time, year, month, week, day, weekday, hour)

""")

# STAGING TABLES

# iam_role = config.get('IAM_ROLE', 'ARN')
# log_data = config.get('S3','LOG_DATA')
# log_json_path = config.get('S3', 'LOG_JSONPATH')
# song_data = config.get('S3', 'SONG_DATA')

staging_events_copy = """
COPY staging_events
FROM {}
CREDENTIALS 'aws_iam_role={}'
FORMAT AS JSON 's3://udacity-dend/log_json_path.json'
REGION 'us-west-2';
""".format(
    config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN']
)

staging_songs_copy = """
COPY staging_songs
FROM {}
CREDENTIALS 'aws_iam_role={}'
FORMAT AS JSON 'auto'
REGION 'us-west-2';
""".format(
    config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN']
)

# staging_events_copy = ("""
# COPY staging_events
# FROM {}
# IAM_ROLE {}
# JSON {}
# """).format(log_data, iam_role, log_json_path)

# staging_songs_copy = ("""
# COPY staging_songs
# FROM {}
# IAM_ROLE {}
# JSON {}
# """).format(song_data, iam_role, "'auto'")

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
    user_agent
    )
SELECT DISTINCT 
        TIMESTAMP 'epoch' + (e.ts / 1000) * INTERVAL '1 second' as start_time
        , e.user_id
        , e.level
        , s.song_id
        , s.artist_id
        , e.sesion_id as session_id
        , e.location 
        , e.user_agent
    FROM staging_events e
    JOIN staging_songs  s
    ON (e.song = s.title AND e.artist_name = s.artist_name AND e.length = s.duration AND e.page = 'NextSong')
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT 
        user_id
        , first_name
        , last_name
        , gender
        , level
FROM staging_events
WHERE (user_id IS NOT NULL AND first_name IS NOT NULL AND last_name IS NOT NULL AND page = 'NextSong')
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT 
        song_id
        , title
        , artist_id
        , year
        , duration
FROM staging_songs
WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT 
        artist_id
        , artist_name
        , artist_location
        , artist_latitude
        , artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT start_time
        , extract(hour from start_time) AS hour
        , extract(day from start_time) AS day
        , extract(week from start_time) AS week
        , extract(month from start_time) AS month
        , extract(year from start_time) AS year
        , extract(dow from start_time) AS weekday
FROM (
        SELECT DISTINCT TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second' AS start_time 
        FROM staging_events
        )
""")

# TESTS:
num_rows_staging_events = 'SELECT COUNT(*) FROM staging_events'
num_rows_staging_songs = 'SELECT COUNT(*) FROM staging_songs'
num_rows_songplays = 'SELECT COUNT(*) FROM songplays'
num_rows_users = 'SELECT COUNT(*) FROM users'
num_rows_songs = 'SELECT COUNT(*) FROM songs'
num_rows_artists = 'SELECT COUNT(*) FROM artists'
num_rows_time = 'SELECT COUNT(*) FROM time'

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
test_queries = [num_rows_staging_events, num_rows_staging_songs, num_rows_songplays, num_rows_users, num_rows_songs, num_rows_artists, num_rows_time]
