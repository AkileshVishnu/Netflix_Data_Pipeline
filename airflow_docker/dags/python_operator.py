from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python_operator import PythonOperator
import tweepy
import pandas as pd
import boto3
import psycopg2

def run_twitter_etl():  
    access_token = "123" 
    access_token_secret = "123" 
    consumer_key = "123"
    consumer_secret = "123"
    
    auth = tweepy.OAuth1UserHandler(
        consumer_key, consumer_secret, access_token, access_token_secret
    )
    
    api = tweepy.API(auth)
    tweets = api.user_timeline(screen_name='@elonmusk',count=200,include_rts = False,tweet_mode = 'extended') 
    list = []
    for tweet in tweets:
        text = tweet._json["full_text"]
    
        refined_tweet = {"user": tweet.user.screen_name,
                        'text' : text,
                        'favorite_count' : tweet.favorite_count,
                        'retweet_count' : tweet.retweet_count,
                        'created_at' : tweet.created_at}
        
        list.append(refined_tweet)
    
    df = pd.DataFrame(list) 
    print("print dataframe")
    print(df.head(10))
    df.to_csv('/opt/airflow/dags/refined_tweets.csv',index=False)



def upload_into_s3(file_name, s3_bucket, s3_key):
    s3 = boto3.client("s3",aws_access_key_id="123",aws_secret_access_key="1232")

    s3.upload_file(
        file_name,
        s3_bucket,
        s3_key
    )
    
with DAG(dag_id="twitter_python_api_v5",default_args={"owner": "sumit"},start_date=datetime(2023, 5, 18), schedule="0 0 * * *",tags=["python_test", ]) as dag:
    
    download_data_from_twitter = PythonOperator(
    task_id='download_data_from_twitter',
    python_callable=run_twitter_etl
    )

    upload_data_into_s3 = PythonOperator(
    task_id='upload_data_into_s3',
    python_callable=upload_into_s3,
    op_args=['/opt/airflow/dags/refined_tweets.csv', 'ec3testbucket007', 'refined_tweets_18_05.csv']
    )
   


    download_data_from_twitter >> upload_data_into_s3
