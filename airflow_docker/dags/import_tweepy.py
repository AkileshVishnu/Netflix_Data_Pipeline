from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python_operator import PythonOperator

def import_tweepy():
    import tweepy
    print(f"print tweepy version {tweepy.__version__}")

# A DAG represents a workflow, a collection of tasks
with DAG(dag_id="import_tweepy_v2",
         default_args={"owner": "Akilesh"}, 
         start_date=datetime(2024, 6, 22), 
         schedule_interval="0 0 * * *",
         tags=["Tweepy",]) as dag:
    # Tasks are represented as operators
    import_tweepy = PythonOperator(
        task_id='import_tweepy',
        python_callable=import_tweepy
    )
    
    import_tweepy
