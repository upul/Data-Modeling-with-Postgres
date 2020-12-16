import os
import glob
import psycopg2
import pandas as pd
import numpy as np
from sql_queries import *
from typing import Union


def _type_converter(data):
    """This is a simple utility method we use for type conversion

    Args:
        data (Union[np.float64, np.float32, np.int64), np.int32, object)]): Data we are going to convert its type

    Returns:
        Union[int, float, object]: data converted to Python type
    """
    if any([isinstance(data, np.float64), isinstance(data, np.float32)]):
        return float(data)
    if any([isinstance(data, np.int64), isinstance(data, np.int32)]):
        return int(data)
    return data


def process_song_file(cur, filepath):
    """This method processes a single song file.

    Args:
        cur (psycopg2.cursor): an instance of Postgres cursor class.
        filepath (str): full path of the song file in JSON format.
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[["song_id", "title", "artist_id", "year", "duration"]].loc[0].values
    song_data = [_type_converter(x) for x in song_data]
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = (
        df[
            [
                "artist_id",
                "artist_name",
                "artist_location",
                "artist_latitude",
                "artist_longitude",
            ]
        ]
        .loc[0]
        .values
    )
    artist_data = [_type_converter(x) for x in artist_data]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """This method processes a single log file.

    Args:
        cur (psycopg2.cursor): an instance of Postgres cursor class.
        filepath (str): full path of the log file in JSON format.
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == "NextSong"]
    df["datetime"] = pd.to_datetime(df.ts, unit="ms")

    # create time DataFrame
    time_df = pd.DataFrame(
        {
            "ts": df.ts,
            "hour": df.datetime.dt.hour,
            "day": df.datetime.dt.day,
            "week_of_year": df.datetime.dt.isocalendar().week,
            "month": df.datetime.dt.month,
            "year": df.datetime.dt.year,
            "weekday": df.datetime.dt.day_name(),
        }
    )

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]
    user_df = user_df.drop_duplicates()

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for _, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (
            row.ts,
            int(row.userId),
            row.level,
            songid,
            artistid,
            row.sessionId,
            row.location,
            row.userAgent,
        )
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """This is a generic function we can use to process either song file or log file

    Args:
        cur (cursor): an instance of Postgres cursor class.
        conn (connection): an instance of Postgres connection class.
        filepath (str): the folder containing your log or user data
        func (object): this could be either process_song_file or process_log_file
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print("{} files found in {}".format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print("{}/{} files processed.".format(i, num_files))


def main():
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student"
    )
    cur = conn.cursor()

    process_data(cur, conn, filepath="data/song_data", func=process_song_file)
    process_data(cur, conn, filepath="data/log_data", func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()