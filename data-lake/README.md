# Database purpose for Sparkify
Sparkify has grown their user base and song database and want to move their processes and data onto the cloud. 

Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. 

So, we can help them by building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

# Sparkify analytical goals
Basically, understands which songs users are listening to.

# Database schema design
Using the song and log datasets, we created a star schema optimized for queries on song play analysis. 
This includes the following Fact, Dimension and Staging tables.

## Fact Table
songplays - records in log data associated with song plays i.e. records with page NextSong 
- songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

## Dimension Tables
users - users in the app 
- user_id, first_name, last_name, gender, level

songs - songs in music database 
- song_id, title, artist_id, year, duration

artists - artists in music database 
- artist_id, name, location, latitude, longitude

time - timestamps of records in songplays broken down into specific units 
- start_time, hour, day, week, month, year, weekday

# ETL pipeline
We are working with two datasets that reside in S3. Here are the S3 links for each:

- Song data: s3://udacity-dend/song_data
- Log data: s3://udacity-dend/log_data

For this ETL we are load credentials from df.cfg, reading data from S3 and loading them back to S3 as parquet files.

# How to run the ETL
- Step 1 - Configure all needed variables in dl.cfg file.
- Step 2 - Execute "python etl.py" (to execute the ETL itself).

# Explanation of the files in the repository
- dl.cfg: Configuration containing AWS credentials.
- etl.py: Python file containing the implementation of the ETL itself, i.e. data related to song and log JSON files being extracted, transformed and loaded as needed.
- test.ipynb: Notebook file created to be used for tests.