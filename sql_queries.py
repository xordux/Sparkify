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
    "     start_time  TIME NOT NULL REFERENCES time, "
    "     user_id     INT NOT NULL REFERENCES users, "
    "     level       TEXT NOT NULL, "
    "     song_id     TEXT NOT NULL REFERENCES songs, "
    "     artist_id   TEXT NOT NULL REFERENCES artists, "
    "     session_id  INT NOT NULL, "
    "     location    TEXT, "
    "     user_agent  TEXT "
    "  );")

user_table_create = (
    "CREATE TABLE users "
    "  ( "
    "     user_id    INT PRIMARY KEY, "
    "     first_name TEXT, "
    "     last_name  TEXT, "
    "     gender     TEXT, "
    "     level      TEXT "
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
    "     start_time TIME PRIMARY KEY, "
    "     hour       INT, "
    "     day        INT, "
    "     week       INT, "
    "     month      INT, "
    "     year       INT, "
    "     weekday    INT "
    "  ); ")

# INSERT RECORDS

song_table_insert = (
    "INSERT INTO"
    "    songs (song_id, title, artist_id, YEAR, duration) "
    "VALUES"
    "    (%s, %s, %s, %s, %s)"
    "    ON CONFLICT (song_id) DO UPDATE "
    "        SET"
    "            title = "
    "            CASE"
    "                WHEN"
    "                    songs.title = '' OR songs.title = 'None' "
    "                THEN"
    "                    EXCLUDED.title "
    "                ELSE"
    "                    songs.title "
    "            END"
    "        , artist_id = "
    "            CASE"
    "                WHEN"
    "                    songs.artist_id = '' OR songs.artist_id = 'None' "
    "                THEN"
    "                    EXCLUDED.artist_id "
    "                ELSE"
    "                    songs.artist_id "
    "            END"
    "        , year = "
    "            CASE"
    "                WHEN"
    "                    songs.year = double precision 'NaN' "
    "                THEN"
    "                    EXCLUDED.year "
    "                ELSE"
    "                    songs.year "
    "            END"
    "        , duration = "
    "            CASE"
    "                WHEN"
    "                    songs.duration = double precision 'NaN' "
    "                THEN"
    "                    EXCLUDED.duration "
    "                ELSE"
    "                    songs.duration "
    "            END;")

artist_table_insert = (
    "INSERT INTO"
    "    artists (artist_id, name, location, latitude, longitude) "
    "VALUES"
    "    (%s, %s, %s, %s, %s)"
    "    ON CONFLICT (artist_id) DO UPDATE"
    "        SET"
    "          name = "
    "            CASE"
    "                WHEN"
    "                    artists.name = '' OR artists.name = 'None' "
    "                THEN"
    "                    EXCLUDED.name "
    "                ELSE"
    "                    artists.name "
    "            END"
    "    , location = "
    "            CASE"
    "                WHEN"
    "                    artists.location = '' OR artists.location = 'None' "
    "                THEN"
    "                    EXCLUDED.location "
    "                ELSE"
    "                    artists.location "
    "            END"
    "    , latitude = "
    "            CASE"
    "                WHEN"
    "                    artists.latitude = DOUBLE PRECISION 'NaN' "
    "                THEN"
    "                    EXCLUDED.latitude "
    "                ELSE"
    "                    artists.latitude "
    "            END"
    "    , longitude = "
    "            CASE"
    "                WHEN"
    "                    artists.longitude = DOUBLE PRECISION 'NaN' "
    "                THEN"
    "                    EXCLUDED.longitude "
    "                ELSE"
    "                    artists.longitude "
    "            END; ")


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
    "INSERT INTO"
    "    time "
    "    SELECT DISTINCT"
    "        On (start_time) * "
    "    FROM"
    "        tmp_time "
    "        ON CONFLICT DO NOTHING;")

insert_from_tmp_users = (
    "INSERT INTO"
    "    users "
    "    SELECT DISTINCT"
    "        On (user_id) * "
    "    FROM"
    "        tmp_users "
    "    WHERE"
    "        user_id IS NOT NULL "
    "        AND level IS NOT NULL "
    "        ON CONFLICT (user_id) DO "
    "        UPDATE"
    "        SET"
    "            level = EXCLUDED.level;")

insert_from_tmp_songplays = (
    "INSERT INTO songplays ("
    "        start_time, "
    "        user_id, "
    "        level, "
    "        song_id, "
    "        artist_id, "
    "        session_id, "
    "        location, "
    "        user_agent ) "
    "SELECT start_time, "
    "       user_id, "
    "       level, "
    "       song_id, "
    "       artist_id, "
    "       session_id, "
    "       location, "
    "       user_agent "
    "FROM   tmp_songplays "
    "WHERE  song_id <> 'None' AND "
    "       artist_id <> 'None';")

# QUERY LISTS

create_table_queries = [user_table_create,
                        song_table_create, artist_table_create,
                        time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop,
                      artist_table_drop, time_table_drop]

insert_from_tmp = [insert_from_tmp_time, insert_from_tmp_users,
                   insert_from_tmp_songplays]
