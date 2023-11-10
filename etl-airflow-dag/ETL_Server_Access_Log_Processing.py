# import the libraries

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
    'ETL_Server_Access_Log_Processing',
    default_args=default_args,
    description='ETL Server Access Log Processing DAG',
    schedule_interval=timedelta(days=1),
)

# ============== Tasks ================ #

# Define the directory where you want to save the downloaded file
download_directory = '/home/fcode/Documents/data-engineer-project/etl-airflow-dag/airflow/dags/'

# download = BashOperator(
#     task_id='download_server_access_log',
#     # Use the -P flag to specify the directory prefix where files will be saved
#     bash_command=f'mkdir -p {download_directory} && wget -P {download_directory} "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt"',
#     dag=dag,
# )

download = BashOperator(
    task_id='download_server_access_log',
    bash_command=f'wget -P {download_directory} "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt" && echo "File downloaded to $(realpath {download_directory}/web-server-access-log.txt)"',
    dag=dag,
)


# download = BashOperator(
#     task_id='download_server_access_log',
#     bash_command='wget "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt"',
#     dag=dag,
# )

'''
The server access log file contains these fields.
a. timestamp - TIMESTAMP
b. latitude - float
c. longitude - float
d. visitorid - char(37)
e. accessed_from_mobile - boolean
f. browser_code - int

The extract task must extract the fields 'timestamp' and 'visitorid'.
'''

# extrack the fields 'timestamp' and 'visitorid'
extract = BashOperator(
    task_id='extract',
    bash_command=f'cut -f1,4 -d"#" {download_directory}/web-server-access-log.txt > {download_directory}/access_log_extracted.txt',
    dag=dag,
)

# The transform task must capitalize the visitorid.
transform = BashOperator(
    task_id='transform',
    bash_command=f'tr "[a-z]" "[A-Z]" < {download_directory}/access_log_extracted.txt > {download_directory}/access_log_capitalized.txt',
    dag=dag,
)

# The load task must compress the extracted and transformed data.
load = BashOperator(
    task_id='load',
    bash_command=f'zip {download_directory}/log.zip {download_directory}/access_log_capitalized.txt' ,
    dag=dag,
)

# Task pipeline
download >> extract >> transform >> load


# To submit the DAG to the Airflow scheduler, run the following command:
#  cp  ETL_Server_Access_Log_Processing.py $AIRFLOW_HOME/dags
# Verify if the DAG is submitted: `airflow dags list`