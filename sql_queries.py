# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = (
    "CREATE TABLE songplays "
    "  ( "
    "     songplay_id SERIAL PRIMARY KEY, "
    "     start_time  TIME NOT NULL, "
    "     user_id     INT NOT NULL, "
    "     level       TEXT NOT NULL, "
    "     song_id     TEXT NOT NULL, "
    "     artist_id   TEXT NOT NULL, "
    "     session_id  INT NOT NULL, "
    "     location    TEXT, "
    "     user_agent  TEXT "
    "  );")

user_table_create = (
    "CREATE TABLE users "
    "  ( "
    "     user_id    INT, "
    "     first_name TEXT, "
    "     last_name  TEXT, "
    "     gender     TEXT, "
    "     level      TEXT, "
    "     PRIMARY KEY(user_id, level)"
    "  ); ")

song_table_create = (
    "CREATE TABLE songs "
    "  ( "
    "     song_id   TEXT PRIMARY KEY, "
    "     title     TEXT, "
    "     artist_id TEXT NOT NULL, "
    "     year      INT, "
    "     duration  NUMERIC "
    "  ); ")

artist_table_create = (
    "CREATE TABLE artists "
    "  ( "
    "     artist_id TEXT PRIMARY KEY, "
    "     name      TEXT, "
    "     location  TEXT, "
    "     latitude  NUMERIC, "
    "     longitude NUMERIC "
    "  ); ")

time_table_create = (
    "CREATE TABLE time "
    "  ( "
    "     start_time TIME, "
    "     hour       INT, "
    "     day        INT, "
    "     week       INT, "
    "     month      INT, "
    "     year       INT, "
    "     weekday    INT "
    "  ); ")

# INSERT RECORDS

songplay_table_insert = (
    "INSERT INTO songplays "
    "            (start_time, "
    "             user_id, "
    "             level, "
    "             song_id, "
    "             artist_id, "
    "             session_id, "
    "             location, "
    "             user_agent) "
    "VALUES      (%s, %s, %s, %s, %s, %s, %s, %s); ")

user_table_insert = (
    "INSERT INTO users "
    "            (user_id, "
    "             first_name, "
    "             last_name, "
    "             gender, "
    "             level)"
    "VALUES     (%s, %s, %s, %s, %s); ")

song_table_insert = (
    "INSERT INTO songs "
    "            (song_id, "
    "             title, "
    "             artist_id, "
    "             YEAR, "
    "             duration) "
    "VALUES     (%s, %s, %s, %s, %s)"
    "ON CONFLICT DO NOTHING; ")

artist_table_insert = (
    "INSERT INTO artists "
    "            (artist_id, "
    "             name, "
    "             location, "
    "             latitude, "
    "             longitude) "
    "VALUES     (%s, %s, %s, %s, %s)"
    "ON CONFLICT DO NOTHING; ")

time_table_insert = (
    "INSERT INTO time "
    "            (start_time, "
    "            hour, "
    "            day, "
    "            week, "
    "            month, "
    "            year, "
    "            weekday) "
    "VALUES    (%s, %s, %s, %s, %s, %s, %s); ")

# FIND SONGS

song_select = (
    "SELECT songs.song_id, "
    "       songs.artist_id "
    "FROM   songs "
    "       INNER JOIN artists "
    "               ON songs.artist_id = artists.artist_id "
    "WHERE  songs.title = %s "
    "       AND artists.name = %s "
    "       AND songs.duration = %s; ")

# Insert from temp
insert_from_tmp_time = (
    "INSERT INTO time "
    "SELECT * "
    "FROM   tmp_time "
    "ON CONFLICT DO NOTHING;")

insert_from_tmp_users = (
    "INSERT INTO users "
    "SELECT DISTINCT "
    "On     (user_id, level) * "
    "FROM   tmp_users "
    "WHERE  user_id IS NOT NULL "
    "AND    level IS NOT NULL "
    "ON CONFLICT DO NOTHING;")

insert_from_tmp_songplays = (
    "INSERT INTO songplays "
    "           (start_time, "
    "            user_id, "
    "            level, "
    "            song_id, "
    "            artist_id, "
    "            session_id, "
    "            location, "
    "            user_agent) "
    "SELECT start_time, "
    "       user_id, "
    "       level, "
    "       song_id, "
    "       artist_id, "
    "       session_id, "
    "       location, "
    "       user_agent "
    "FROM   tmp_songplays "
    "ON CONFLICT DO NOTHING;")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create,
                        song_table_create, artist_table_create,
                        time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop,
                      artist_table_drop, time_table_drop]

insert_from_tmp = [insert_from_tmp_time, insert_from_tmp_users,
                   insert_from_tmp_songplays]
