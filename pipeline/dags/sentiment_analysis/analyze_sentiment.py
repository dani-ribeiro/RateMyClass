import pandas as pd
import os
import logging
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

def analyze_sentiment():
    current_directory = os.path.dirname(os.path.abspath(__file__))
    reviews_file_path = os.path.join(current_directory, '../reviews.csv')
    
    reviews_df = pd.read_csv(reviews_file_path)
    
    if 'REVIEW' not in reviews_df.columns:
        raise KeyError("The 'REVIEW' column is missing from the reviews file.")
    
    intensity_analyzer = SentimentIntensityAnalyzer()
    
    def calculate_sentiment(text):
        try:
            sentiment_score = intensity_analyzer.polarity_scores(text)['compound']
            return sentiment_score
        except Exception as e:
            logging.info(f"Error processing review: {text}, error: {e}")
            return 0
    
    reviews_df['SENTIMENT_SCORE'] = reviews_df['REVIEW'].apply(calculate_sentiment)
    
    reviews_df.to_csv(reviews_file_path, index=False)
    
