#!/bin/bash

echo "extract_transform_load"

# Directory where you want to download and work on the file
dir="/home/fcode/Documents/data-engineer-project/etl-airflow-dag-bash/airflow/dags"

# URL of the file to be downloaded
url="https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt"

# The path to the downloaded file
downloaded_file="$dir/web-server-access-log.txt"

# Use wget to download the file
wget -O "$downloaded_file" "$url"

# cut command to extract the fields timestamp and visitorid writes to a new file extracted.txt
cut -f1,4 -d"#" "$downloaded_file" > "$dir/extracted.txt"

# tr command to transform by capitalizing the visitorid.
tr "[a-z]" "[A-Z]" < "$dir/extracted.txt" > "$dir/capitalized.txt"

# tar command to compress the extracted and transformed data.
tar -czvf "$dir/log.tar.gz" -C "$dir" capitalized.txt
