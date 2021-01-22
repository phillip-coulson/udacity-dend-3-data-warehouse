# Project: Data Warehouse

## Overview

The purpose of this project is to apply what I've learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. This project involves loading data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.
 
## Project Structure

- create_table.py is where I've created fact and dimension tables for the star schema in Redshift.
- etl.py is where I've loaded data from S3 into staging tables on Redshift and then process that data into analytics tables on Redshift.
- sql_queries.py is where I've  defined SQL statements, which will be imported into the two other files above.
- README.md is where I've provided discussion on process and decisions for this ETL pipeline.

## Tables
The database contains the following fact and dimension tables:

### Fact Table
- songplays - records in event data associated with song plays i.e. records with page NextSong (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)

### Dimension Tables
- users - users in the app (user_id, first_name, last_name, gender, level)
- songs - songs in music database (song_id, title, artist_id, year, duration)
- artists - artists in music database (artist_id, name, location, lattitude, longitude)
- time - timestamps of records in songplays broken down into specific units (start_time, hour, day, week, month, year, weekday)

# Config
This pipeline requires a configuration file (dwh.cfg) with the following structure:
[CLUSTER]
HOST=<your_host>
DB_NAME=<your_db_name>
DB_USER=<your_db_user>
DB_PASSWORD=<your_db_password>
DB_PORT=<your_db_port>

[IAM_ROLE]
ARN=<your_iam_role_arn>

[S3]
LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'

# Running the Pipeline
To run the pipeline on an existing cluster, enter the following on the command line (note - working directory should be the top-level folder):

python3 create_tables.py
python3 etl.py