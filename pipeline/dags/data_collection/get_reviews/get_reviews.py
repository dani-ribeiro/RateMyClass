import requests
import json
import csv
from datetime import datetime
import logging
import os

def get_reviews():
    current_directory = os.path.dirname(os.path.abspath(__file__))
    professors_file_path = os.path.join(current_directory, 'professors.json')
    reviews_file_path = os.path.join(current_directory, '../../reviews.csv')

    with open(professors_file_path, 'r') as file:
        data = json.load(file)
        professors = data["search"]["teachers"]["edges"]

    reviews = []

    for count, professor in enumerate(professors):
        professor_info = professor["node"]

        print(f'Fetching Reviews for Professor #{count}: {professor_info["firstName"]} {professor_info["lastName"]}')
        logging.info(f'Fetching Reviews for Professor #{count}: {professor_info["firstName"]} {professor_info["lastName"]}')
        
        # get professor ID and number of ratings for grabbing reviews
        professorID = professor_info['legacyId']
        num_ratings = professor_info['numRatings']
        page = 1
        remaining_reviews = num_ratings

        while remaining_reviews > 0:
            url = f'https://www.ratemyprofessors.com/paginate/professors/ratings?tid={professorID}&page={page}'
            response = requests.get(url)
            
            if response.status_code == 200:
                print(f'Page {page}')
                logging.info(f'Page {page}')

                data = response.json()
                ratings = data.get('ratings', [])
                for rating in ratings:
                    review = {
                        'School ID': 1147, # rating['sId']
                        'School Name': professor_info['school']['name'],
                        'Professor ID': professorID,
                        'Professor Name': f"{professor_info['firstName']} {professor_info['lastName']}",
                        'Overall Quality': professor_info['avgRating'],
                        'Overall Difficulty': professor_info['avgDifficulty'],
                        'Department': professor_info['department'],
                        'Review ID': rating['id'],
                        'Course Code': rating['rClass'],
                        'Review Date': datetime.strptime(rating['rDate'], '%m/%d/%Y').date(),
                        'Quality': rating['rOverall'],
                        'Difficulty': rating['rEasy'],
                        'Review Text': rating['rComments'].strip(),
                        'Would Take Again': True if rating.get('rWouldTakeAgain') == 'Yes' else (False if rating.get('rWouldTakeAgain') == 'No' else None),
                        'Grade': rating.get('teacherGrade', None),
                        'Attendance': rating.get('attendance', None),
                        'Textbook Usage': True if rating.get('rTextBookUse') == 'Yes' else (False if rating.get('rTextBookUse') == 'No' else None),
                        'Thumbs Up': rating['helpCount'],
                        'Thumbs Down': rating['notHelpCount'],
                    }
                    reviews.append(review)
                
                # update remaining number of reviews
                remaining_reviews = data.get('remaining', 0)
                page += 1
            else:
                print(f'Error fetching page {page} for professorID {professorID}: {response.status_code}')
                logging.info(f'Error fetching page {page} for professorID {professorID}: {response.status_code}')
                break

    # write everything to csv
    with open(reviews_file_path, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['School ID', 'School Name', 'Professor ID', 'Professor Name', 'Overall Quality', 'Overall Difficulty', \
                    'Department', 'Review ID', 'Course Code', 'Review Date', 'Quality', 'Difficulty', 'Review Text', \
                        'Would Take Again', 'Grade', 'Attendance', 'Textbook Usage', 'Thumbs Up', 'Thumbs Down']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for review in reviews:
            writer.writerow(review)

    print('Reviews have been successfully written to reviews.csv')
    logging.info('Reviews have been successfully written to reviews.csv')

def main():
    get_reviews()

if __name__ == "__main__":
    main()