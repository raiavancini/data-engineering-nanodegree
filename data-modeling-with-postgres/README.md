# Database purpose for Sparkify
Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. 

Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. 

So, we can solve their pain point by creating a Postgres database with tables designed to optimize queries on song play analysis and a ETL pipeline to populated it using the JSON metadata. 

# Sparkify analytical goals
Basically, understands what songs users are listening to.

# Database schema design
Using the song and log datasets, we created a star schema optimized for queries on song play analysis. 
This includes the following Fact and Dimension tables.

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
The created ETL will process data by getting each JSON file and extracting all needed information from it. 
Then, some data transformation is performed and the result is loaded in a Postgres database.

# How to run the ETL
- Step 1 - Execute "python create_tables.py" (to create all needed tables in the database) 
- Step 2 - Execute "python etl.py" (to execute the ETL itself)

# Explanation of the files in the repository
- data: Folder containing JSON files to be processed in the ETL. 
- create_tables.py: Python file containing implemented functions responsible for the SQL tables creation and deletion. 
- etl.ipynb: Jupyter Notebook file used to implement the initial version of the solution and make test for the ETL process. 
- etl.py: Python file containing the implementation of the ETL itself, i.e. data related to song and log JSON files being extracted, transformed and loaded as needed. 
- sql_queries.py: Python file containing all SQL queries needed for the implemented solution (CRUD in general). 
- test.ipynb: Jupyter Notebook file used to perform tests and validations during the implementation. 
