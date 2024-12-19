import pandas as pd
from fuzzywuzzy import process
import os
import logging

def similar_course_mapper(department):
    """
    uses fuzzy matching NLP to group Course Code within each department that share at least a 92% similarity.
    standardizes course codes to a single common course code, if they're similar enough
    """

    unique_courses = department['Course Code'].unique()
    course_map = {}
    
    for course in unique_courses:
        # find the closest matches to each course within the department
        matches = process.extract(course, unique_courses)
        # group courses together if they have a similarity score >= 92/100%
        similar_courses = [match for match, score in matches if score >= 92]
        
        if similar_courses:
            # if a similar course grouping exists --> the new course code will be the most popular course code of that group
            most_frequent_course = department[department['Course Code'].isin(similar_courses)]['Course Code'].mode()
            if len(most_frequent_course) > 0:
                most_frequent_course_value = most_frequent_course[0]
                for similar_course in similar_courses:
                    course_map[similar_course] = most_frequent_course_value
                    
    return course_map

def correct_courses(row, corrections):
    return corrections.get(row['Department'], {}).get(row['Course Code'], row['Course Code'])

def clean_data():
    current_directory = os.path.dirname(os.path.abspath(__file__))
    reviews_file_path = os.path.join(current_directory, '../reviews.csv')
    data = pd.read_csv(reviews_file_path)
    
    # convert course codes to uppercase
    data['Course Code'] = data['Course Code'].str.upper()

    # remove rows with empty or null Course Code
    data = data[data['Course Code'].notna() & (data['Course Code'].str.strip() != '')]

    # only allow course codes that match the following format: [A-Z]+[0-9]{3,4}[A-Z]?
    # start with alphabetical letters, followed by 3-4 digits, and an optional alphabetical letter indicating a type of course (i.e. A, T, M, S, etc.)
    # Examples: 
        # Accepted Course Codes: CSE330S, CSE217A, CSE240
        # Not Accepted Course Codes: 100B, 100, B
    course_code_pattern = r'^[A-Z]+[0-9]{3,4}[A-Z]?$'
    data = data[data['Course Code'].str.contains(course_code_pattern, regex=True)]
    
    # remove trailing letters from the Course Code
    # Example: CHEM111A --> CHEM111
    data['Course Code'] = data['Course Code'].str.replace(r'[A-Z]$', '', regex=True)

    # standardizes course codes by department
    course_corrections = data.groupby('Department').apply(similar_course_mapper).to_dict()
    
    # correct course codes
    data['Course Code'] = data.apply(correct_courses, corrections=course_corrections, axis=1)

    # rename all columns
    data.rename(columns=
        {
            'School ID': 'SCHOOL_ID',
            'School Name': 'SCHOOL_NAME',
            'Professor ID': 'PROFESSOR_ID',
            'Professor Name': 'PROFESSOR_NAME',
            'Overall Quality': 'OVERALL_QUALITY',
            'Overall Difficulty': 'OVERALL_DIFFICULTY',
            'Department': 'DEPARTMENT_NAME',
            'Review ID': 'REVIEW_ID',
            'Course Code': 'COURSE_CODE',
            'Review Date': 'DATE',
            'Quality': 'QUALITY',
            'Difficulty': 'DIFFICULTY',
            'Review Text': 'REVIEW',
            'Would Take Again': 'WOULD_TAKE_AGAIN',
            'Grade': 'GRADE',
            'Attendance': 'ATTENDANCE',
            'Textbook Usage': 'TEXTBOOK_USAGE',
            'Thumbs Up': 'THUMBS_UP',
            'Thumbs Down': 'THUMBS_DOWN',
        }, inplace=True)
    
    # create department_id by auto-incrementing on unique department names
    data['DEPARTMENT_ID'] = data['DEPARTMENT_NAME'].astype('category').cat.codes + 1
    
    # create class_id by auto-incrementing on unique course_codes
    data['CLASS_ID'] = data['COURSE_CODE'].astype('category').cat.codes + 1
    
    # save clean data
    data.to_csv(reviews_file_path, index=False)
    logging.info('Reviews have been successfully cleaned')

if __name__ == "__main__":
    clean_data()
