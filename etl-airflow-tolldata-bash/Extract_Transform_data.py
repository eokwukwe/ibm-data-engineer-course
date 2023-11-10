from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'Matinez Lopez',
    'start_date': days_ago(0),
    'email': ['matinez.lopez@mail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'Extract_Transform_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment - Runngin Bash Script',
    schedule_interval=timedelta(days=1),
)

shell_script="/home/fcode/airflow/dags/Extract_Transform_data.sh"

# Tasks
extract_transform_load = BashOperator(
    dag=dag,
    task_id='extract_transform_load',
    bash_command="/home/fcode/airflow/dags/Extract_Transform_data.sh ",
)

# Task pipeline
extract_transform_load