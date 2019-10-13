import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, monotonically_increasing_id

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates a spark session.
    
    returns a spark session
    """    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """
    Loads json song files for spark processesing.
    Produces artists_table and songs_table.
    Writes parquet format versions to S3.
    
    spark - spark connection
    input_data - read data path
    output_data - write data path
    
    returns nothing
    """
    
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json" 
    
    # read song data file
    df = spark.read.json(song_data)
    df.printSchema()
    df.show(5)
    df.createOrReplaceTempView("songs")
    
    # extract columns to create songs table
    songs_table = spark.sql("""
    SELECT DISTINCT song_id, title, artist_id, duration, year 
    FROM songs
    ORDER BY song_id
    """)
    print(songs_table.show())
    
    # write songs table to parquet files partitioned by year and artist
    song_path = output_data + "songs_table"
    print("writing songs to " + song_path)
    songs_table.write.partitionBy("year","artist_id").mode('overwrite').parquet(song_path)

    # extract columns to create artists table
    # use the latest (i.e. most recent) artist record
    artists_table = spark.sql("""
        WITH CTE AS (
            SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude, count(1) as total 
            , ROW_NUMBER () OVER( PARTITION BY artist_id ORDER BY artist_id) as RN
            FROM songs
            group by artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        )
        SELECT DISTINCT artist_id, artist_name as name, artist_location as location, artist_latitude as latitude, artist_longitude as longitude
        FROM CTE
        WHERE RN = 1    
    """)
    print(artists_table.show())

    # write artists table to parquet files
    artists_path = output_data + "artists_table"
    print("writing artists to " + artists_path)
    artists_table.write.mode('overwrite').parquet(artists_path)

def process_log_data(spark, input_data, output_data):
    """
    Loads json log files for spark processesing.
    Produces user_table, time_table, songplays_table.
    Writes parquet format versions to S3.
    
    spark - spark connection
    input_data - read data path
    output_data - write data path
    
    returns nothing
    """

    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df["page"] == "NextSong")
    df.printSchema()
    df.show(5)
    df.createOrReplaceTempView("logs")

    # extract columns for users table    
    # users appear multiple times with different levels, select the last version of the record as determined by the timestamp field
    users_table = spark.sql("""
        WITH CTE AS(
            SELECT DISTINCT userId, firstName, lastName, gender, level
            , ROW_NUMBER () OVER( PARTITION BY userID ORDER BY ts DESC) as RN
            FROM logs 
            WHERE userid IS NOT NULL
        )
        SELECT userId as user_id, firstName as first_name, lastName as last_name, gender, level 
        FROM CTE
        WHERE RN = 1
    """)
    print(users_table.show())

    # write users table to parquet files
    users_path = output_data + "users_table"
    print("writing users to " + users_path)
    users_table.write.mode('overwrite').parquet(users_path)
    
    # extract columns to create time table
    time_table = spark.sql("""
    SELECT
        time_stamp,
        EXTRACT(HOUR FROM time_stamp) As hour,
        EXTRACT(DAY FROM time_stamp) As day,
        EXTRACT(WEEK FROM time_stamp) As week,
        EXTRACT(MONTH FROM time_stamp) As month,
        EXTRACT(YEAR FROM time_stamp) As year,
        date_format(time_stamp, 'EEEE') AS weekday
    FROM
        (SELECT DISTINCT
            from_unixtime(ts/1000) as time_stamp
        FROM
            logs
        )
    """) 
    print(time_table.show())
    
    # write time table to parquet files partitioned by year and month
    time_path = output_data + "time_table"
    print("writing time to " + time_path)
    time_table.write.partitionBy("year","month").mode('overwrite').parquet(time_path)

    # read in song data to use for songplays table
    songs_df = spark.read.parquet(output_data + 'songs_table')
    artists_df = spark.read.parquet(output_data + 'artists_table')
    print(artists_df.show(5))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
    SELECT DISTINCT from_unixtime(ts/1000) as time_stamp, userid as user_id, level, sessionid as session_id, location, useragent as user_agent, song, artist
    FROM logs
    WHERE userid IS NOT NULL
    """) 
    songplays_table = songplays_table.withColumn("songplay_id", monotonically_increasing_id())
    songplays_table = songplays_table.join(artists_df, songplays_table.artist==artists_df.name, "LeftOuter").select(songplays_table.songplay_id,
                                                                                                             songplays_table.time_stamp, 
                                                                                                             songplays_table.user_id, 
                                                                                                             songplays_table.level, 
                                                                                                             songplays_table.session_id, 
                                                                                                             songplays_table.location, 
                                                                                                             songplays_table.user_agent, 
                                                                                                             songplays_table.song, 
                                                                                                             artists_df.artist_id)
    print(songplays_table.show())
    
    songplays_table = songplays_table.join(songs_df, songplays_table.song==songs_df.title, "LeftOuter").select(songplays_table.songplay_id, 
                                                                                                               songplays_table.time_stamp, 
                                                                                                               songplays_table.user_id, 
                                                                                                               songplays_table.level, 
                                                                                                               songplays_table.artist_id, 
                                                                                                               songs_df.song_id,
                                                                                                               songplays_table.session_id, 
                                                                                                               songplays_table.location, 
                                                                                                               songplays_table.user_agent)
    print(songplays_table.show())

    # write songplays table to parquet files partitioned by year and month
    songplays_path = output_data + "songplays_table"
    print("writing songplays to " + songplays_path)

    #https://stackoverflow.com/questions/35437378/spark-save-dataframe-partitioned-by-virtual-column
    dt = col("time_stamp").cast("date")
    fname = [(year, "year"), (month, "month")]
    exprs = [col("*")] + [f(dt).alias(name) for f, name in fname]
    songplays_table.select(*exprs).write.partitionBy(*(name for _, name in fname)).mode('overwrite').parquet(songplays_path)
    
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""

    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

