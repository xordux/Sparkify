import os
import glob
import psycopg2
import pandas as pd
from io import StringIO
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Populates Songs and Artists table

    This method extracts information from JSON file present in filepath and
    inserts it into Table Songs and Artists defined in Database using cur.

    :param cur: cursor of connected database
    :param filepath: Path of the json file contain song info
    :return: None
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[["song_id", "title", "artist_id", "year",
                    "duration"]].values[0].tolist()
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = df[["artist_id", "artist_name", "artist_location",
                     "artist_latitude", "artist_longitude"]].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Populates Time, Users and Songplays table

    This method extracts information from JSON file present in filepath and
    inserts it into Table Time and Users defined in Database using cur.
    It also uses information in Songs and Artists table to insert information
    in Songplays table.

    :param cur: cursor of connected database
    :param filepath: Path of the json file contain song info
    :return: None
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df["page"] == "NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')

    # insert time data records
    time_data = (
        t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = (
        "start_time", "hour", "day", "week", "month", "year", "weekday")
    time_df = pd.concat(time_data, axis=1, keys=column_labels)

    data = StringIO(time_df.to_csv(index=False, header=False))
    cur.copy_from(data, 'time', sep=",", columns=(
        "start_time", "hour", "day", "week", "month", "year", "weekday"))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]
    # insert user records
    data = StringIO(user_df.to_csv(index=False, header=False))
    cur.copy_from(data, 'users', sep=",", columns=(
        "user_id", "first_name", "last_name", "gender", "level"))

    data = []
    # insert songplay records
    for index, row in df.iterrows():
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        print(str(row["ts"]) + " " + str(pd.to_datetime(row["ts"])))
        songplay_data = map(str, (
            pd.to_datetime(row["ts"]), row["userId"], row["level"], songid,
            artistid, row["sessionId"], row["location"],
            row["userAgent"]))
        data.append("|".join(songplay_data))

    cols = (
        "START_TIME", "USER_ID", "LEVEL", "SONG_ID", "ARTIST_ID", "SESSION_ID",
        "LOCATION", "USER_AGENT")
    data = StringIO("\n".join(data))
    cur.copy_from(data, 'songplays', sep="|", columns=cols)


def process_data(cur, conn, filepath, func):
    """
    Finds JSON files in filepath(and subfolders) and calls func using it.

    :param cur: cursor of connected database
    :param conn: connection to database
    :param filepath: folder containing JSON files
    :param func: function to process JSON file
    :return: None
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def remove_duplicates(cur, table_name):
    """
    Removes all the duplicate records in a table.

    This method:
        1. Copies the original table into a temporary table.
        2. Drops the original table.
        3. Recreates the original table from duplicate table but only selects
           the distinct rows.
    Duplicate row is defined as any row which has all fields identical to
    another row.

    :param cur: cursor of connected database
    :param table_name: Name of the table which might have duplicate rows
    :return: None
    """
    drop_temp = "DROP TABLE IF EXISTS tmp_table;"
    cur.execute(drop_temp)

    temp_create = (
        "CREATE temp TABLE tmp_table "
        "  on commit DROP "
        "AS "
        "  SELECT * "
        f"  FROM   {table_name}; ")
    cur.execute(temp_create)

    drop_main = f"DROP TABLE {table_name};"
    cur.execute(drop_main)

    create_main = (
        f"CREATE TABLE {table_name} "
        f"AS "
        f"SELECT DISTINCT * "
        f"from tmp_table;")
    cur.execute(create_main)


def main():
    """ Connects to Database and runs process_data() """
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    tab_pk_dict = {"users": "user_id",
                   "songs": "song_id",
                   "artists": "artist_id"}
    for table in tab_pk_dict:
        remove_duplicates(cur, table)

    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
