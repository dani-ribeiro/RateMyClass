from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from utils.file_checks import check_professors_file, check_reviews_file, check_cleaned_reviews_file, check_analyzed_reviews_file
from data_collection.get_reviews.get_reviews import get_reviews
from data_cleaning.clean_data import clean_data
from data_storage.store_data import store_data
from airflow.operators.dummy import DummyOperator   # used to skip tasks (temporary debugging purposes)
import os

# disable proxy to allow web requests
os.environ['NO_PROXY'] = '*'

# pipeline.py's file path
BASE_DIRECTORY = os.path.dirname(os.path.abspath(__file__))

default_args = {
    'owner': 'airflow'
}

with DAG(
    dag_id='pipeline',
    default_args=default_args,
    start_date=datetime(2023, 11, 1),
    schedule_interval=None,  # or '@daily'
    catchup=False,
) as dag:

    # data collection task group [data_collection] = [get_professors] -> [check_professors_file] -> [get_reviews] -> [check_reviews_file]
    with TaskGroup(group_id='data_collection') as data_collection:
        
        # task: get_professors
        # gets information for every professor at WashU and writes to get_reviews/professors.json
        get_professors = BashOperator(
            task_id='get_professors',
            bash_command=f"cd {os.path.join(BASE_DIRECTORY, 'data_collection/get_professors')} && npx ava"
        )

        # task: check_professors_file
        # checks if professors.json exists before proceeding
        check_professors_file = PythonOperator(
            task_id='check_professors_file',
            python_callable=check_professors_file
        )

        # task: get_reviews
        # gets all reviews for every professor at WashU and writes to data_cleaning/reviews.csv
        get_reviews = PythonOperator(
            task_id='get_reviews',
            python_callable=get_reviews
        )

        # use this to skip get_reviews task
        # get_reviews = DummyOperator(
        #     task_id='get_reviews'
        # )

        # task: check_reviews_file
        # checks if reviews.csv exists before proceeding
        check_reviews_file = PythonOperator(
            task_id='check_reviews_file',
            python_callable=check_reviews_file
        )

        get_professors >> check_professors_file >> get_reviews >> check_reviews_file

    
    # data cleaning task group [data_cleaning] = [clean_data] -> [check_reviews_file]
    with TaskGroup(group_id='data_cleaning') as data_cleaning:
        # task: clean_data
        clean_data = PythonOperator(
            task_id='clean_data',
            python_callable=clean_data
        )

        # task: check_reviews_file
        # checks if the cleaned reviews.csv exists before proceeding
        check_cleaned_reviews_file = PythonOperator(
            task_id='check_cleaned_reviews_file',
            python_callable=check_cleaned_reviews_file
        )

        clean_data >> check_cleaned_reviews_file

    # sentiment analysis task group [sentiment_analysis] = [analyze_sentiment] -> [check_analyzed_reviews_file]
    with TaskGroup(group_id='sentiment_analysis') as sentiment_analysis:
        # TODO
        # task: analyze_sentiment
        # perform sentiment analysis on each review, add sentiment score as a new column and save to reviews.csv
        # analyze_sentiment = PythonOperator(
        #     task_id='analyze_sentiment',
        #     python_callable=analyze_sentiment
        # )

        # task: check_analyzed_reviews_file
        # checks if the analyzed reviews.csv exists before proceeding
        check_analyzed_reviews_file = PythonOperator(
            task_id='check_analyzed_reviews_file',
            python_callable=check_analyzed_reviews_file
        )

        # analyze_sentiment >> check_analyzed_reviews_file
        check_analyzed_reviews_file

    # task: data_storage
    # organizes/normalizes data into multiple tables and uploads to snowflake
    data_storage = PythonOperator(
        task_id='data_storage',
        python_callable=store_data
    )

    data_collection >> data_cleaning >> sentiment_analysis >> data_storage
