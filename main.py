import tweepy as tw
import re
import pandas as pd
import time
from local import utils
from local import cloudutils
from datetime import datetime


def run(search_words, date_since):

    print("Getting tweets...")
    #get tweets using the API
    tweet_df = utils.getTweets(consumer_key,consumer_secret,access_token,access_token_secret,search_words,date_since)

    #print(tweet_df.head(2))
    print("Getting polarity scores...")

    tweet_df_sentiment = utils.getPolarityScores(tweet_df)

    print("Getting sentiment...")
    tweet_df_sentiment['sentiment'] = tweet_df_sentiment['polarity_scores'].apply(lambda x: utils.getSentimentValue(x))

    tweet_df_sentiment['compound'] = tweet_df_sentiment['polarity_scores'].apply(lambda x: x['compound'])

    tweet_df_sentiment.drop(['polarity_scores'],axis=1)
    #print(tweet_df_sentiment.head(5))

    destination_file_uri = cloudutils.WriteToGCS(tweet_df_sentiment)
    time.sleep(5)
    cloudutils.WriteToBQ(destination_file_uri)
    print("GCS WRITE:- --- SUCCESS ---")
    

def main(request):

    search_words = "#ipl2022"
    date_since = "2022-03-21"
    
    date_since = datetime.now()
    date_since = str(date_since).split(' ')[0]
    
    search_words = getsearchwords(date_since);
    

    run(search_words,date_since)