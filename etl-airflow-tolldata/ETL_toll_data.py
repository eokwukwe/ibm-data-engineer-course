from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Okwukwe Ewurum',
    'start_date': days_ago(0),
    'email': ['okwukwe.ewurum@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

staging_dir = '/home/fcode/airflow/dags/finalassignment/staging'
download_dir = '/home/fcode/airflow/dags/finalassignment'

downloaded_file = f'{download_dir}/tolldata.tgz'

# ============== Tasks ================ #

# unzip_data Task
unzip_data = BashOperator(
    dag=dag,
    task_id='unzip_data',
    bash_command=f'tar -xvzf {downloaded_file} -C {download_dir}',
)

# extract_data_from_csv task
'''
This task should extract the fields Rowid, Timestamp, Anonymized Vehicle number, 
and Vehicle type from the vehicle-data.csv file and save them into a file named 
csv_data.csv
'''
extract_data_from_csv = BashOperator(
    dag=dag,
    task_id='extract_data_from_csv',
    bash_command=f'cut -d"," -f1-4 {download_dir}/vehicle-data.csv > {download_dir}/csv_data.csv',
)

# extract_data_from_tsv task
'''
This task should extract the fields Number of axles, Tollplaza id, and Tollplaza 
code from the tollplaza-data.tsv file and save it into a file named tsv_data.csv
'''
extract_data_from_tsv = BashOperator(
    dag=dag,
    task_id='extract_data_from_tsv',
    bash_command=f"cut -d$'\t' -f5-7 {download_dir}/tollplaza-data.tsv | tr '\t' ',' > {download_dir}/tsv_data.csv",
)


# extract_data_from_fixed_width task
'''
This task should extract the fields Type of Payment code, and Vehicle Code from the fixed width file 
payment-data.txt and save it into a file named fixed_width_data.csv
'''
extract_data_from_fixed_width = BashOperator(
    dag=dag,
    task_id='extract_data_from_fixed_width',
    bash_command=f"awk '{{ print $(NF-1) \",\" $NF }}' {download_dir}/payment-data.txt > {download_dir}/fixed_width_data.csv",
)

# consolidate_data task
'''
This task should create a single csv file named extracted_data.csv by combining 
data from the following files:

- csv_data.csv
- tsv_data.csv
- fixed_width_data.csv

The final csv file should use the fields in the order given below:

Rowid, Timestamp, Anonymized Vehicle number, Vehicle type, Number of axles, Tollplaza id, 
Tollplaza code, Type of Payment code, and Vehicle Code
'''
consolidate_data = BashOperator(
    dag=dag,
    task_id='consolidate_data',
    bash_command=f"paste -d\",\" {download_dir}/csv_data.csv {download_dir}/tsv_data.csv {download_dir}/fixed_width_data.csv > {download_dir}/extracted_data.csv"
)

# transform_data task
'''
This task should transform the vehicle_type field in extracted_data.csv into capital letters and 
save it into a file named transformed_data.csv in the staging directory
'''
transform_data = BashOperator(
    dag=dag,
    task_id='transform_data',
    bash_command=f"awk 'BEGIN {{FS=\",\"; OFS=\",\"}} {{print $1, $2, $3, toupper($4), $5, $6, $7}}' {download_dir}/extracted_data.csv > {staging_dir}/transformed_data.csv",
)


# Task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data