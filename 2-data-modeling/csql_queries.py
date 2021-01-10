# DROP TABLES

sessions_table_drop = 'DROP TABLE IF EXISTS sessions;'
user_sessions_table_drop = 'DROP TABLE IF EXISTS user_sessions;'
song_listeners_table_drop = 'DROP TABLE IF EXISTS song_listeners;'


# CREATE TABLES

# sessions table
## PRIMARY KEY chosen because:
## - session_id as partition key ensures an even distribution of data across partitions
## - item_in_session will be queried on so needs to be a clustering column
sessions_table_create = '''
    CREATE TABLE IF NOT EXISTS sessions (
        session_id int,
        item_in_session int,
        artist text,
        song_title text,
        song_length float,
        PRIMARY KEY (session_id, item_in_session)
    )
    '''

## user_sessions table
## PRIMARY KEY chosen because:
## - user_id as partition key ensures an even distribution of data across partitions
## - session_id will be queried on so needs to be a clustering column
## - results should be sorted by item_in_session, which is efficiently achieved by having it as a clustering column
user_sessions_table_create = '''
    CREATE TABLE IF NOT EXISTS user_sessions (
        user_id int,
        session_id int,
        item_in_session int,
        artist text,
        song_title text,
        user_first_name text,
        user_last_name text,
        PRIMARY KEY (user_id, session_id, item_in_session)
    );
    '''

## song_listeners table
## PRIMARY KEY chosen because:
## - song_title as partition key as this is the only column being queried with a WHERE clause
## - user_id as a clustering column to ensure exactly one row per user who listens to each song
song_listeners_table_create = '''
    CREATE TABLE IF NOT EXISTS song_listeners (
        song_title text,
        user_id int,
        user_first_name text,
        user_last_name text,
        PRIMARY KEY (song_title, user_id)
    );
    '''


# INSERT RECORDS

sessions_table_insert = '''
    INSERT INTO sessions (session_id, item_in_session, artist, song_title, song_length)
    VALUES (%s, %s, %s, %s, %s)
    '''

user_sessions_table_insert = '''
    INSERT INTO user_sessions (
        user_id, session_id, item_in_session, artist, song_title, user_first_name, user_last_name
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    '''

song_listeners_table_insert = '''
    INSERT INTO song_listeners (
        song_title, user_id, user_first_name, user_last_name
    )
    VALUES (%s, %s, %s, %s)
    '''


# FIND QUERIES

# Query 1
# "Give me the artist, song title and song's length in the music app history
# that was heard during sessionId = 338, and itemInSession = 4"
query_1 = '''
    SELECT artist, song_title, song_length FROM sessions
    WHERE session_id = 338 AND item_in_session = 4;
    '''

# Query 2
# "Give me only the following: name of artist, song (sorted by itemInSession) 
# and user (first and last name) for userid = 10, sessionid = 182"
query_2 = '''
    SELECT artist, song_title, user_first_name, user_last_name FROM user_sessions
    WHERE user_id = 10 and session_id = 182
    '''

# Query 3
# "Give me every user name (first and last) in my music app history
# who listened to the song 'All Hands Against His Own'"
query_3 = '''
    SELECT user_first_name, user_last_name FROM song_listeners
    WHERE song_title = 'All Hands Against His Own'
    '''


# QUERY LISTS

queries = { 'Query 1': query_1, 'Query 2': query_2, 'Query 3': query_3 }
create_table_queries = [sessions_table_create, user_sessions_table_create, song_listeners_table_create]
drop_table_queries = [sessions_table_drop, user_sessions_table_drop, song_listeners_table_drop]