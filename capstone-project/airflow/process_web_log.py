import os
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'Okwukwe Ewurum',
    'start_date': days_ago(0),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'Process_Web_Log',
    default_args=default_args,
    description='Pipeline to analyze the web server log file',
    schedule_interval=timedelta(days=1),
)

airflow_home = os.environ['AIRFLOW_HOME']
dag_dir = f'{airflow_home}/dags/capstone'

# Extract the ipaddress field from the web server log file and save it into 
# a file named extracted_data.txt
extract_data = BashOperator(
  dag=dag,
  task_id='extract_data',
  bash_command=f'cut -f1,1 -d" " {dag_dir}/accesslog.txt > {dag_dir}/extracted_data.txt'
)

# Filter out all the occurrences of ipaddress “198.46.149.143”
# from extracted_data.txt and save the output to a file transformed_data.txt
transform_data = BashOperator(
  dag=dag,
  task_id='transform_data',
  bash_command=f'grep -v "198.46.149.143" {dag_dir}/extracted_data.txt > {dag_dir}/transformed_data.txt'
)


# Archive the file transformed_data.txt into a tar file named weblog.tar
load_data = BashOperator(
  dag=dag,
  task_id='load_data',
  bash_command=f'tar -cvf {dag_dir}/weblog.tar {dag_dir}/transformed_data.txt'
)

# Task pipeline
extract_data >> transform_data >> load_data


