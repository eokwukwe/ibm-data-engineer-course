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
    'ETL_Server_Access_Log_Processing_Bash',
    default_args=default_args,
    description='ETL Server Access Log Processing DAG Using An External Bash Script',
    schedule_interval=timedelta(days=1),
)

# ============== Tasks ================ #

# define the task named extract_transform_and_load to call the shell script
extract_transform_and_load = BashOperator(
    task_id="extract_transform_and_load",
    bash_command="/home/fcode/airflow/dags/ETL_Server_Access_Log_Processing_Bash.sh ",
    dag=dag,
)

# Task pipeline
extract_transform_and_load