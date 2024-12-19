from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from utils.file_checks import check_professors_file, check_reviews_file, check_cleaned_reviews_file, check_analyzed_reviews_file
from data_collection.get_reviews.get_reviews import get_reviews
from data_cleaning.clean_data import clean_data
from data_storage.store_data import store_data
from sentiment_analysis.analyze_sentiment import analyze_sentiment
from airflow.operators.dummy import DummyOperator   # used to skip tasks (temporary debugging purposes)
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.hooks.base import BaseHook
import os

# snowflake connection
snowflake_conn_id = 'snowflake_default'
conn_details = BaseHook.get_connection(snowflake_conn_id)
DATABASE = conn_details.extra_dejson.get('database')
SCHEMA = conn_details.extra_dejson.get('schema')

# fact and dimension tables
FACT_REVIEW = 'fact_review'
DIM_CLASS = 'dim_class'
DIM_PROFESSOR = 'dim_professor'
DIM_SCHOOL = 'dim_school'
DIM_DEPARTMENT = 'dim_department'

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
        analyze_sentiment = PythonOperator(
            task_id='analyze_sentiment',
            python_callable=analyze_sentiment
        )

        # task: check_analyzed_reviews_file
        # checks if the analyzed reviews.csv exists before proceeding
        check_analyzed_reviews_file = PythonOperator(
            task_id='check_analyzed_reviews_file',
            python_callable=check_analyzed_reviews_file
        )

        analyze_sentiment >> check_analyzed_reviews_file

    # task: data_storage
    # organizes/normalizes data into multiple tables and uploads to snowflake
    data_storage = PythonOperator(
        task_id='data_storage',
        python_callable=store_data
    )

    # task: data_transformation
    # transforms normalized data into fact and dimension tables for OLAP
    data_transformation = SQLExecuteQueryOperator(
        task_id='data_transformation',
        conn_id=snowflake_conn_id,
        sql=f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.{SCHEMA}.{FACT_REVIEW} AS (
            SELECT
                r.REVIEW_ID AS REVIEW_ID,
                r.CLASS_ID AS CLASS_ID,
                r.PROFESSOR_ID AS PROFESSOR_ID,
                c.DEPARTMENT_ID AS DEPARTMENT_ID,
                c.SCHOOL_ID AS SCHOOL_ID,
                r.QUALITY AS QUALITY,
                r.DIFFICULTY AS DIFFICULTY,
                r.REVIEW AS REVIEW,
                r.DATE AS DATE,
                r.WOULD_TAKE_AGAIN AS WOULD_TAKE_AGAIN,
                r.GRADE AS GRADE,
                r.ATTENDANCE AS ATTENDANCE,
                r.TEXTBOOK_USAGE AS TEXTBOOK_USAGE,
                (r.THUMBS_UP - r.THUMBS_DOWN) AS NET_THUMBS_UP,
                r.SENTIMENT_SCORE AS SENTIMENT_SCORE
            FROM {DATABASE}.{SCHEMA}.reviews AS r
            JOIN {DATABASE}.{SCHEMA}.classes AS c ON c.CLASS_ID = r.CLASS_ID
        );

        CREATE TABLE IF NOT EXISTS {DATABASE}.{SCHEMA}.{DIM_CLASS} AS
            SELECT 
                class_ID,
                course_code
            FROM {DATABASE}.{SCHEMA}.classes;

        CREATE TABLE IF NOT EXISTS {DATABASE}.{SCHEMA}.{DIM_PROFESSOR} AS
            SELECT 
                professor_ID,
                professor_name
            FROM {DATABASE}.{SCHEMA}.professors;

        CREATE TABLE IF NOT EXISTS {DATABASE}.{SCHEMA}.{DIM_SCHOOL} AS
            SELECT 
                school_ID,
                school_name
            FROM {DATABASE}.{SCHEMA}.schools;

        CREATE TABLE IF NOT EXISTS {DATABASE}.{SCHEMA}.{DIM_DEPARTMENT} AS
            SELECT 
                department_ID,
                department_name
            FROM {DATABASE}.{SCHEMA}.departments;
        """
    )

    data_collection >> data_cleaning >> sentiment_analysis >> data_storage >> data_transformation
