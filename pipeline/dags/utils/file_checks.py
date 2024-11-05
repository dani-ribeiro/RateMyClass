import logging
import os

def check_professors_file():
    """checks if get_reviews/professors.json exists"""

    current_directory = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_directory, '../data_collection/get_reviews/professors.json')
    if not os.path.exists(file_path):
        logging.error('ERROR: get_reviews/professors.json not found.')
        raise FileNotFoundError('ERROR: get_reviews/professors.json not found.')
    else:
        logging.info('SUCCESS: professors.json exists')

def check_reviews_file():
    """checks if reviews.csv exists"""

    current_directory = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_directory, '../reviews.csv')
    if not os.path.exists(file_path):
        logging.error('ERROR: reviews.csv not found.')
        raise FileNotFoundError('ERROR: reviews.csv not found.')
    else:
        logging.info('SUCCESS: reviews.csv exists')

def check_cleaned_reviews_file():
    """checks if reviews.csv exists"""

    current_directory = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_directory, '../reviews.csv')
    if not os.path.exists(file_path):
        logging.error('ERROR: reviews.csv not found.')
        raise FileNotFoundError('ERROR: reviews.csv not found.')
    else:
        logging.info('SUCCESS: reviews.csv exists')

def check_analyzed_reviews_file():
    """checks if reviews.csv exists"""

    current_directory = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_directory, '../reviews.csv')
    if not os.path.exists(file_path):
        logging.error('ERROR: reviews.csv not found.')
        raise FileNotFoundError('ERROR: reviews.csv not found.')
    else:
        logging.info('SUCCESS: reviews.csv exists')