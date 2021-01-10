import configparser

config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = 'DROP TABLE IF EXISTS songplays;'
user_table_drop = 'DROP TABLE IF EXISTS users;'
song_table_drop = 'DROP TABLE IF EXISTS songs;'
artist_table_drop = 'DROP TABLE IF EXISTS artists;'
time_table_drop = 'DROP TABLE IF EXISTS time;'


# CREATE TABLES

staging_events_table_create = '''
    CREATE TABLE staging_events(
        artist TEXT,
        auth TEXT,
        firstName TEXT,
        gender TEXT,
        itemInSession INT,
        lastName TEXT,
        length FLOAT,
        level TEXT,
        location TEXT,
        method TEXT,
        page TEXT,
        registration BIGINT,
        sessionId INT,
        song TEXT,
        status INT,
        ts BIGINT,
        userAgent TEXT,
        userId INT 
    );
    '''

staging_songs_table_create = '''
    CREATE TABLE staging_songs(
        num_songs INT,
        artist_id TEXT,
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_location TEXT,
        artist_name TEXT,
        song_id TEXT,
        title TEXT,
        duration FLOAT,
        year INT
    );
    '''

songplay_table_create = '''
    CREATE TABLE songplays (
        songplay_id INT IDENTITY(0, 1) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL,
        user_id int NOT NULL, 
        level TEXT NOT NULL,
        song_id TEXT NOT NULL,
        artist_id TEXT NOT NULL,
        session_id INT,
        location TEXT,
        user_agent TEXT
    );
    '''

user_table_create = '''
    CREATE TABLE users (
        user_id INT PRIMARY KEY,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        gender TEXT NOT NULL,
        level TEXT NOT NULL
    );
    '''

song_table_create = '''
    CREATE TABLE songs (
        song_id TEXT PRIMARY KEY,
        title TEXT NOT NULL,
        artist_id TEXT NOT NULL,
        year INT,
        duration FLOAT NOT NULL
    );
    '''

artist_table_create = '''
    CREATE TABLE artists (
        artist_id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        location TEXT,
        latitude FLOAT,
        longitude FLOAT
    );
    '''

time_table_create = '''
    CREATE TABLE time (
        start_time TIMESTAMP PRIMARY KEY,
        hour INT NOT NULL,
        day INT NOT NULL,
        week INT NOT NULL,
        month INT NOT NULL,
        year INT NOT NULL,
        weekday INT NOT NULL
    );
    '''


# COPY TABLES

staging_events_copy = '''
    COPY staging_events
    FROM '{}'
    IAM_ROLE '{}'
    FORMAT AS JSON '{}';
    '''.format(
      config['S3']['LOG_DATA'], 
      config['IAM_ROLE']['ARN'], 
      config['S3']['LOG_JSONPATH']
    )

staging_songs_copy = '''
    COPY staging_songs
    FROM '{}'
    IAM_ROLE '{}'
    FORMAT AS JSON 'auto';
    '''.format(
      config['S3']['SONG_DATA'], 
      config['IAM_ROLE']['ARN']
    )


# INSERT RECORDS

songplay_table_insert = '''
    INSERT INTO songplays (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    )
    SELECT
        TIMESTAMP 'epoch' + (events.ts / 1000) * INTERVAL '1 second' AS start_time,
        events.userId AS user_id, 
        events.level AS level, 
        songs.song_id AS song_id, 
        songs.artist_id AS artist_id, 
        events.sessionId AS session_id, 
        events.location AS location, 
        events.userAgent AS user_agent
    FROM staging_events AS events
    JOIN staging_songs AS songs
    ON (events.song = songs.title AND events.artist = songs.artist_name)
    AND events.page = 'NextSong';
    '''

user_table_insert = '''
    INSERT INTO users (
        user_id,
        first_name,
        last_name,
        gender,
        level
    )
    SELECT
        userId AS user_id,
        MAX(firstName) AS first_name,
        MAX(lastName) AS last_name,
        MAX(gender) as gender,
        MAX(level) as level
    FROM staging_events
    WHERE user_id IS NOT NULL
    AND page = 'NextSong'
    GROUP BY userId;
    '''

song_table_insert = '''
    INSERT INTO songs (
        song_id,
        title,
        artist_id,
        year,
        duration
    )
    SELECT DISTINCT
        song_id,
        MAX(title) as title,
        MAX(artist_id) as artist_id,
        MAX(year) as year,
        MAX(duration) as duration
    FROM staging_songs
    WHERE song_id IS NOT NULL
    GROUP BY song_id;
    '''

artist_table_insert = '''
    INSERT INTO artists (
        artist_id,
        name,
        location,
        latitude,
        longitude
    )
    SELECT DISTINCT
        artist_id,
        MAX(artist_name) AS name,
        MAX(artist_location) AS location,
        MAX(artist_latitude) AS latitude,
        MAX(artist_longitude) AS longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL
    GROUP BY artist_id;
    '''

time_table_insert = '''
    INSERT INTO time (
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
    )
    WITH timestamp AS (
      SELECT DISTINCT
          TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second' AS start_time
      FROM staging_events
    )
    SELECT
        start_time,
        EXTRACT(hour FROM start_time) AS hour,
        EXTRACT(day FROM start_time) AS day,
        EXTRACT(week FROM start_time) AS week,
        EXTRACT(month FROM start_time) AS month,
        EXTRACT(year FROM start_time) AS year,
        EXTRACT(dayofweek FROM start_time) AS weekday
    FROM timestamp;
    '''


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
