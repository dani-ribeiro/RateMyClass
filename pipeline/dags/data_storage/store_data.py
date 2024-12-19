import pandas as pd
import os
import logging
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas
from airflow.hooks.base import BaseHook
from snowflake.connector import connect

def organize_data():
    """creates and loads pandas dataframes using reviews.csv"""

    current_directory = os.path.dirname(os.path.abspath(__file__))
    reviews_file_path = os.path.join(current_directory, '../reviews.csv')
    data = pd.read_csv(reviews_file_path)
    reviews_df = data[
        [
            'REVIEW_ID',
            'CLASS_ID',
            'PROFESSOR_ID',
            'QUALITY',
            'DIFFICULTY',
            'REVIEW',
            'DATE',
            'WOULD_TAKE_AGAIN',
            'GRADE',
            'ATTENDANCE',
            'TEXTBOOK_USAGE',
            'SENTIMENT_SCORE',
            'THUMBS_UP',
            'THUMBS_DOWN'
        ]
    ]

    classes_df = data[
        [
            'CLASS_ID',
            'SCHOOL_ID',
            'DEPARTMENT_ID',
            'COURSE_CODE'
        ]
    ].drop_duplicates()

    professors_df = data[
        [
            'PROFESSOR_ID',
            'PROFESSOR_NAME',
            'OVERALL_QUALITY',
            'OVERALL_DIFFICULTY'
        ]
    ].drop_duplicates()

    schools_df = data[
        [
            'SCHOOL_ID',
            'SCHOOL_NAME'
        ]
    ].drop_duplicates()

    departments_df = data[
        [
            'DEPARTMENT_ID',
            'DEPARTMENT_NAME'
        ]
    ].drop_duplicates()

    class_instructors_df = data[
        [
            'CLASS_ID',
            'PROFESSOR_ID'
        ]
    ].drop_duplicates()

    return {
        'SCHOOLS': schools_df,
        'DEPARTMENTS': departments_df,
        'PROFESSORS': professors_df,
        'CLASSES': classes_df,
        'REVIEWS': reviews_df,
        'CLASS_INSTRUCTORS': class_instructors_df
    }

def upload_to_snowflake(dataframes):
    """creates tables and uploads data to Snowflake"""
    
    # get snowflake connection from airflow
    snowflake_conn_id = 'snowflake_default'
    conn_details = BaseHook.get_connection(snowflake_conn_id)

    conn = connect(
        user=conn_details.login,
        password=conn_details.password,
        account=conn_details.extra_dejson.get('account'),
        warehouse=conn_details.extra_dejson.get('warehouse'),
        database=conn_details.extra_dejson.get('database'),
        schema=conn_details.extra_dejson.get('schema')
    )

    try:
        # create tables & upload data to snowflake
        table_creation_queries = {
            "schools": """
                CREATE OR REPLACE TABLE schools (
                    school_id INTEGER PRIMARY KEY,
                    school_name VARCHAR NOT NULL
                )
            """,
            "departments": """
                CREATE OR REPLACE TABLE departments (
                    department_id INTEGER PRIMARY KEY,
                    department_name VARCHAR NOT NULL
                )
            """,
            "professors": """
                CREATE OR REPLACE TABLE professors (
                        professor_id INTEGER PRIMARY KEY,
                        professor_name VARCHAR NOT NULL,
                        overall_quality FLOAT NOT NULL,
                        overall_difficulty FLOAT NOT NULL
                )
            """,
            "classes": """
                CREATE OR REPLACE TABLE classes (
                    school_id INTEGER,
                    department_id INTEGER,
                    class_id INTEGER PRIMARY KEY,
                    course_code VARCHAR NOT NULL,
                    FOREIGN KEY (school_id) REFERENCES schools(school_id),
                    FOREIGN KEY (department_id) REFERENCES departments(department_id)
                )
            """,
            "reviews": """
                CREATE OR REPLACE TABLE reviews (
                    review_id INTEGER PRIMARY KEY,
                    class_id INTEGER,
                    professor_id INTEGER,
                    quality FLOAT NOT NULL,
                    difficulty FLOAT NOT NULL,
                    review TEXT,
                    date DATE NOT NULL,
                    would_take_again BOOLEAN,
                    grade VARCHAR,
                    attendance VARCHAR,
                    textbook_usage BOOLEAN,
                    thumbs_up INTEGER,
                    thumbs_down INTEGER,
                    sentiment_score FLOAT,
                    FOREIGN KEY (class_id) REFERENCES classes(class_id),
                    FOREIGN KEY (professor_id) REFERENCES professors(professor_id)
                )
            """,
            "class_instructors": """
                CREATE OR REPLACE TABLE class_instructors (
                    class_id INTEGER NOT NULL,
                    professor_id INTEGER NOT NULL,
                    FOREIGN KEY (class_id) REFERENCES classes(class_id),
                    FOREIGN KEY (professor_id) REFERENCES professors(professor_id),
                    PRIMARY KEY (class_id, professor_id)
                )
            """
        }

        with conn.cursor() as cursor:
            # create & use warehouse, db, schema if not exists
            cursor.execute(f'CREATE WAREHOUSE IF NOT EXISTS {conn_details.extra_dejson.get("warehouse")}')
            cursor.execute(f'ALTER WAREHOUSE {conn_details.extra_dejson.get("warehouse")} SET WAREHOUSE_SIZE=XSmall')
            cursor.execute(f'USE WAREHOUSE {conn_details.extra_dejson.get("warehouse")}')
            cursor.execute(f'CREATE DATABASE IF NOT EXISTS {conn_details.extra_dejson.get("database")}')
            cursor.execute(f'USE DATABASE {conn_details.extra_dejson.get("database")}')
            cursor.execute(f'CREATE SCHEMA IF NOT EXISTS {conn_details.extra_dejson.get("schema")}')
            cursor.execute(f'USE SCHEMA {conn_details.extra_dejson.get("schema")}')

            # create tables
            for table, query in table_creation_queries.items():
                cursor.execute(query)
                logging.info(f"Table '{table}' successfully created")

            # upload data
            for table_name, dataframe in dataframes.items():
                write_pandas(conn, dataframe, table_name)
                logging.info(f"Table '{table_name}' successfully uploaded to Snowflake")

    except Exception as e:
        logging.error("ERROR:", e)
        raise
    
    finally:
        conn.close()

def store_data():
    dataframes = organize_data()
    upload_to_snowflake(dataframes)

if __name__ == "__main__":
    store_data()