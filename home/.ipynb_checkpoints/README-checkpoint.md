# Project Summary
Create a data lake for use by PySpark to support song play analysis, including the creation of the an analytic schema and ETL pipeline, for the Sparkify analytics team. The included python script creates a file based star schema in the Amazon S3 storage service of song plays for analysis purposes. The data is from http://millionsongdataset.com/ and is in JSON format.

The schema for the datalake ...
Fact Table
- songplays - records in log data associated with song plays
    - songplay_id (unique for for each song play)
    - start_time (for joining with the time files)
    - user_id (for joining with the user files)
    - level
    - song_id (for joining with the song files)
    - artist_id (for joining with the artists files)
    - session_id
    - location
    - user_agent

Dimension Tables
- users - Users in the app. Over time the user's level can change. Current design is to use the most recent user record. A recommended modification would be to use begin and end dates for behavioral analysis purposes. In other words, to understand how a given user's usage changes depending on their level, we would need the dates each level began and ended.
    - user_id (unique identifer)
    - first_name
    - last_name
    - gender
    - level

- songs - songs in music database
    - song_id (unique identifer)
    - title
    - artist_id foreign key artist table
    - year
    - duration

- artists - Artists in music database. An individual artist can have multiple names (e.g. Linkin Park, Linkin Park featuring Steve Akoi). The code is designed to select the most common or frequent version. I.e. Linkin Park, not Link Park featuring Steve Akoi). Ideally the upsteam system would generate separate artist ids to represent Linkin Park and Linkin Park featuring Steve Akoi.
    - artist_id (unique identifer)
    - name
    - location
    - lattitude
    - longitude

- time - Timestamps of records in songplays broken down into specific units.
    - time_stamp (unique identifer)
    - hour
    - day
    - week
    - month
    - year
    - weekday
    
# Explanation of Files in Repository

## etl.py
Contains the routines to process the JSON files and insert the data into the tables. You will need to update the output data location.

## dl.cfg
Configuration file for AWS. Replace with your own values.

# Explanation of data (stored on AWS S3)

### song data
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

### log data
![Image of Log](https://s3.amazonaws.com/video.udacity-data.com/topher/2019/February/5c6c15e9_log-data/log-data.png)


# Process to Follow

To build your database named sparkifydb (userid = student and password = student) for songplay analysis ...

1. Update configuration file with your own AWS information 
2. Update etl.py with your S3 storage location
3. execute the command python etl.py

# Future suggestions

1. Test whether ts performs better than time_stamp.