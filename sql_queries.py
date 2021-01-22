import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_ROLE_ARN = config.get("IAM_ROLE","ARN")
LOG_DATA=config.get("S3","LOG_DATA")
LOG_JSONPATH=config.get("S3","LOG_JSONPATH")
SONG_DATA=config.get("S3","SONG_DATA")

# DROP TABLE STATEMENTS

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLE STATEMENTS

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs 
        (
            num_songs int, 
            artist_id varchar, 
            artist_latitude float, 
            artist_longitude float, 
            artist_location varchar, 
            artist_name varchar, 
            song_id varchar, 
            title varchar, 
            duration float, 
            year int   
        );
""")

staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events 
        (
            artist varchar, 
            auth varchar, 
            firstName varchar, 
            gender varchar, 
            iteminSession int, 
            lastName varchar, 
            length numeric, 
            level varchar, 
            location varchar, 
            method varchar, 
            page varchar, 
            registration numeric, 
            sessionId int, 
            song varchar, 
            status int, 
            ts numeric, 
            userAgent varchar, 
            userId int
        );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays 
        (
            songplay_id int primary key, 
            start_time timestamp not null, 
            user_id int not null, 
            level varchar not null, 
            song_id varchar not null, 
            artist_id varchar not null, 
            session_id int not null, 
            location varchar, 
            user_agent varchar not null
        );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users 
        (
            user_id int primary key, 
            first_name varchar not null, 
            last_name varchar not null, 
            gender varchar not null, 
            level varchar not null
        );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs 
        (
            song_id varchar primary key, 
            title varchar not null, 
            artist_id varchar not null, 
            year int not null, 
            duration float not null
        );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists 
        (
            artist_id varchar primary key, 
            name varchar not null, 
            location varchar, 
            latitude float, 
            longitude float
        );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time 
        (
            start_time timestamp primary key, 
            hour int not null, 
            day int not null, 
            week int not null, 
            month int not null, 
            year int not null, 
            weekday int not null
        );
""")

# COPY TO STAGING TABLE STATEMENTS

staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    format as json {} 
    ;
""").format(LOG_DATA, DWH_ROLE_ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    json 'auto' 
    ;
""").format(SONG_DATA, DWH_ROLE_ARN)

# INSERT INTO STATEMENTS

songplay_table_insert = ("""
    INSERT INTO songplays 
        (
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
        timestamp 'epoch' + es.ts / 1000 * interval '1 second' AS start_time,
        es.userId as user_id,
        es.level,
        ss.song_id,
        ss.artist_id,
        es.sessionId as session_id,
        es.location,
        es.userAgent as user_agent
    FROM staging_events es
    LEFT JOIN staging_songs ss ON es.song = ss.title AND es.artist = ss.artist_name
    WHERE es.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users
    SELECT DISTINCT
        se.userId, 
        se.firstName, 
        se.lastName, 
        se.gender, 
        se.level
    FROM staging_events se
    JOIN 
        (
            SELECT max(ts) AS ts, 
            userId
            FROM staging_events
            WHERE page = 'NextSong'
            GROUP BY userId
        ) ei 
    ON se.userId = ei.userId AND se.ts = ei.ts
""")

song_table_insert = ("""
    INSERT INTO songs
    SELECT DISTINCT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
""")

artist_table_insert = ("""
    INSERT INTO artists
    SELECT DISTINCT
        artist_id,
        artist_name as name,
        artist_location as location,
        artist_latitude as latitude,
        artist_longitude as longitude
    FROM staging_songs
""")

time_table_insert = ("""
    INSERT INTO time
    SELECT
        time.start_time,
        EXTRACT(hour FROM time.start_time) AS hour,
        EXTRACT(day FROM time.start_time) AS day,
        EXTRACT(week FROM time.start_time) AS week,
        EXTRACT(month FROM time.start_time) AS month,
        EXTRACT(year FROM time.start_time) AS year,
        EXTRACT(weekday FROM time.start_time) AS weekday
    FROM (
        SELECT DISTINCT
            timestamp 'epoch' + ts / 1000 * interval '1 second' AS start_time
        FROM staging_events
        WHERE page = 'NextSong'
    ) time
""")
# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
