from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator

# A DAG represents a workflow, a collection of tasks
with DAG(dag_id="demo",
         default_args={"owner": "Akilesh"}, 
         start_date=datetime(2024, 6, 22), 
         schedule_interval="0 0 * * *",
         tags=["Airflow_test",]) as dag:
    # Tasks are represented as operators
    hello = BashOperator(task_id="hello", bash_command="echo hello")

    @task()
    def airflow():
        print("airflow")

    # Set dependencies between tasks
    hello >> airflow()