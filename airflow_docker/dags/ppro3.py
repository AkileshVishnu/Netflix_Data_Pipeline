# dag.py

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pandas as pd
import snowflake_utils as sf_utils
import json

# Default arguments for the DAG
default_args = {
    'owner': 'Akilesh',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'Netflix_Data_Pipeline',
    default_args=default_args,
    description='A simple DAG to insert data into Snowflake',
    schedule_interval=timedelta(days=1),
)

# Snowflake connection parameters
connection_params = {
    'user': 'mohanraj',
    'password': 'Akilesh@1997',
    'account': 'MOHHJKJ-EOB44343',
    'warehouse': 'COMPUTE_WH',
    'database': 'ppro_airflow_db',
    'schema': 'ppro_airflow_schema'
}

# Define Python callable functions
def load_csv(**kwargs):
    """Load the CSV file into a DataFrame."""
    file_path = '/opt/airflow/data_files/Countries_by_GDP.csv'
    df = pd.read_csv(file_path)
    df_json = df.to_json(orient='split')
    kwargs['ti'].xcom_push(key='df_json', value=df_json)

def create_table(**kwargs):
    """Create the table in Snowflake."""
    df_json = kwargs['ti'].xcom_pull(key='df_json', task_ids='load_csv_task')
    df = pd.read_json(df_json, orient='split')
    columns = sf_utils.infer_column_definitions(df)
    sf_utils.create_snowflake_table(connection_params, columns)

def insert_data(**kwargs):
    """Insert data into the Snowflake table."""
    df_json = kwargs['ti'].xcom_pull(key='df_json', task_ids='load_csv_task')
    df = pd.read_json(df_json, orient='split')
    sf_utils.insert_data_to_snowflake(connection_params, df)

# Define the tasks
load_csv_task = PythonOperator(
    task_id='load_csv_task',
    python_callable=load_csv,
    provide_context=True,
    dag=dag,
)

create_table_task = PythonOperator(
    task_id='create_table_task',
    python_callable=create_table,
    provide_context=True,
    dag=dag,
)

insert_data_task = PythonOperator(
    task_id='insert_data_task',
    python_callable=insert_data,
    provide_context=True,
    dag=dag,
)

dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='cd /opt/airflow/dbt/netflix_project && dbt run',
    dag=dag,
)

# Set the task dependencies
load_csv_task >> create_table_task >> insert_data_task >> dbt_run
