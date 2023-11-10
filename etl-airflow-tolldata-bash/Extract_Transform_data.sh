#!/bin/bash

download_dir='/home/fcode/airflow/dags/finalassignment'
staging_dir='/home/fcode/airflow/dags/finalassignment/staging'

# Extract data from vehicle-data.csv. extract the fields Rowid, Timestamp, Anonymized Vehicle number,
# and Vehicle type from the vehicle-data.csv file and save them into a file named csv_data.csv
cut -d"," -f1-4 "$download_dir/vehicle-data.csv" > "$download_dir/csv_data.csv"

# extract the fields Number of axles, Tollplaza id, and Tollplaza code from the 
# tollplaza-data.tsv file and save it into a file named tsv_data.csv.
cut -d$'\t' -f5-7 "$download_dir/tollplaza-data.tsv" | tr '\t' ',' > "$download_dir/tsv_data.csv"

# extract the fields Type of Payment code, and Vehicle Code from the fixed width 
# file payment-data.txt and save it into a file named fixed_width_data.csv.
awk '{ print $(NF-1) "," $NF }' "$download_dir/payment-data.txt" > "$download_dir/fixed_width_data.csv"

# create a single csv file named extracted_data.csv by combining data from the following files: 
# csv_data.csv, tsv_data.csv, and fixed_width_data.csv. 
# The order of the files should be csv_data.csv, tsv_data.csv, and fixed_width_data.csv. 
# The final final csv file should use the fields in the order: Rowid, Timestamp, 
# Anonymized Vehicle number, Vehicle type, Number of axles, Tollplaza id, Tollplaza code, 
# Type of Payment code, and Vehicle Code.
paste -d"," "$download_dir/csv_data.csv" "$download_dir/tsv_data.csv" "$download_dir/fixed_width_data.csv" > "$download_dir/extracted_data.csv"

#  transform the vehicle_type field (column 4) in extracted_data.csv into capital letters 
# and save it into a file named transformed_data.csv 
awk 'BEGIN {FS=","; OFS=","} {print $1, $2, $3, toupper($4), $5, $6, $7}' "$download_dir/extracted_data.csv" > "$staging_dir/transformed_data.csv"

echo "Done"
