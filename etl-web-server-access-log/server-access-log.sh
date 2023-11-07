#!/bin/bash

# cp-access-log.sh
# This script downloads the file 'web-server-access-log.txt.gz'
# from "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Bash%20Scripting/ETL%20using%20shell%20scripting/".

# The script then extracts the .txt file using gunzip.

# The .txt file contains the timestamp, latitude, longitude 
# and visitor id apart from other data.

# Transforms the text delimeter from "#" to "," and saves to a csv file.
# Loads the data from the CSV file into the table 'access_log' in PostgreSQL database.

# Download the access log file
echo "Downloading data"
wget "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Bash%20Scripting/ETL%20using%20shell%20scripting/web-server-access-log.txt.gz"

# Unzip the file to extract the .txt file.
echo "Unziping file"
gunzip -f web-server-access-log.txt.gz

# Extract phase
echo "Extracting data"
# Extract the columns 1 (timestamp), 2 (latitude), 3 (longitude) and 
# 4 (visitorid)
cut -d"#" -f1-4 web-server-access-log.txt > extracted-web-server-acess-log.txt 

# Transform phase
echo "Transforming data"

# Read the extracted data and replace the hashs with commas
tr "#", "," < extracted-web-server-acess-log.txt > web-server-access-log.csv

# # Load phase
# echo "Loading data"

# Send the instructions to connect to 'template1' and copy the file to the table
# 'users' throgh command pipeline
echo "TRUNCATE TABLE access_log; COPY access_log FROM '/home/fcode/Documents/data-engineer-project/etl-web-server-access-log/web-server-access-log.csv' DELIMITERS ',' CSV HEADER;" | psql postgresql://admin:fcodeman@localhost:5432/template1
