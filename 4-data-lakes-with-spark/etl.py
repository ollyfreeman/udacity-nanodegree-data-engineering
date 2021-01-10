import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['KEY']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['SECRET']

def create_spark_session():
    '''
    Creates a spark session, or retrieves the current one if one exists
  
    Returns the spark session

    Parameters:
        None

    Returns:
        None
    '''
    spark = SparkSession \
        .builder \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.0') \
        .getOrCreate()

    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Loads JSON song data from S3 and saves the relevant parquet tables to S3

    Parameters:
        spark: The spark session
        input_data: The top-level location of the input data
        output_data: The top-level location of the output data

    Returns:
        None
    '''

    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    song_fields = ['song_id', 'title', 'artist_id', 'year', 'duration']
    songs_table = df.select(song_fields).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(output_data + 'songs/', 'overwrite')

    # extract columns to create artists table
    artists_fields = [
      'artist_id', 'artist_name as name', 'artist_location as location',
      'artist_latitude as latitude', 'artist_longitude as longitude',
    ]
    artists_table = df.selectExpr(artists_fields).dropDuplicates()

    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists/', 'overwrite')


def process_log_data(spark, input_data, output_data):
    '''
    Loads JSON log data from S3 and saves the relevant parquet tables to S3

    Parameters:
        spark: The spark session
        input_data: The top-level location of the input data
        output_data: The top-level location of the output data

    Returns:
        None
    '''

    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_columns = ['userId as user_id', 'firstName as first_name', 'lastName as last_name', 'gender', 'level']
    users_table = df.selectExpr(users_columns).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users/', 'overwrite')

    # create timestamp column from original timestamp column
    get_datetime = udf(lambda ts: datetime.fromtimestamp(ts / 1000), TimestampType())
    df = df.withColumn('start_time', get_datetime('ts'))
    
    # extract columns to create time table
    time_table = time_table = df.select(
        hour('start_time').alias('hour'),
        dayofmonth('start_time').alias('day'),
        weekofyear('start_time').alias('week'),
        month('start_time').alias('month'),
        year('start_time').alias('year'),
        date_format('start_time', 'u').alias('weekday')
    ).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(output_data + 'time/', 'overwrite')

    # read in song data to use for songplays table
    songs_df = spark.read.parquet(output_data + 'songs/*/*/*.parquet')
    artists_df = spark.read.parquet(output_data + 'artists/*.parquet')

    # extract columns from joined song and log datasets to create songplays table 
    logs_songs_df = df \
        .join(songs_df, (df.song == songs_df.title))
    logs_songs_artists_df = logs_songs_df \
        .join(artists_df, (logs_songs_df.artist == artists_df.name)) \
        .drop(artists_df.location)

    songplays_table = logs_songs_artists_df.select(
        monotonically_increasing_id().alias('songplay_id'),
        'start_time',
        col('userId').alias('user_id'),
        'level',
        'song_id',
        'artist_id',
        col('sessionId').alias('session_id'),
        'location',
        col('userAgent').alias('user_agent'),
        year('start_time').alias('year'), # for partitioning
        month('start_time').alias('month') # for partitioning
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(output_data + 'songplays/', 'overwrite')


def main():
    spark = create_spark_session()
    input_data = 's3a://udacity-dend/'
    output_data = 's3a://' + config['S3']['OUTPUT_BUCKET'] + '/'
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == '__main__':
    main()
