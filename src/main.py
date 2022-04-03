import tweepy as tw
import re
import pandas as pd
import time
import os
from local import utils
from local import cloudutils
from local import matches
from datetime import datetime


consumer_key = os.environ.get('consumer_key', 'Specified environment variable is not set.')
consumer_secret = os.environ.get('consumer_secret', 'Specified environment variable is not set.')
access_token = os.environ.get('access_token', 'Specified environment variable is not set.')
access_token_secret= os.environ.get('access_token_secret', 'Specified environment variable is not set.')


def run(search_words, date_since):

    print("Getting tweets...")
    #get tweets using the API
    maxTweetId = cloudutils.getMaxTweetId(date_since,search_words[1].replace("#",""))

    tweet_df = utils.getTweets(consumer_key,consumer_secret,access_token,access_token_secret,search_words,date_since,maxTweetId)

    #print(tweet_df.head(2))
    print("Getting polarity scores...")

    tweet_df_sentiment = utils.getPolarityScores(tweet_df)

    print("Getting sentiment...")
    tweet_df_sentiment['sentiment'] = tweet_df_sentiment['polarity_scores'].apply(lambda x: utils.getSentimentValue(x))

    tweet_df_sentiment['compound'] = tweet_df_sentiment['polarity_scores'].apply(lambda x: x['compound'])

    tweet_df_sentiment.drop(['polarity_scores'],axis=1)
    #print(tweet_df_sentiment.head(5))

    
    tweet_df_sentiment['match'] = pd.Series([search_words[1].replace("#","") for x in range(len(tweet_df_sentiment.index))])
    
    destination_file_uri = cloudutils.WriteToGCS(tweet_df_sentiment)
    time.sleep(5)
    cloudutils.WriteToBQ(destination_file_uri)
    print("GCS WRITE:- --- SUCCESS ---")
    
    maxTweetId = tweet_df['id'].max()
    
    cloudutils.insertMaxTweetId(date_since,search_words[1].replace("#",""),maxTweetId)

def main(request, data):

    search_words = ["#ipl2022"]
    
    date_since = datetime.now()
    date_since = str(date_since).split(' ')[0]
    
    match_on_date = matches.getMatch(date_since)
    
    today = datetime.utcnow()
    hour = today.time().hour
    
    if len(match_on_date) == 2:
        print("Two Match Day")
        print(match_on_date)

        today = datetime.utcnow()
        hour = today.time().hour
        
        if hour >= 10 and hour < 14:
            #Match 1
            search_words.append(match_on_date[0])
            run(search_words,date_since)
            search_words.remove(match_on_date[0])
        elif hour >= 14:
            #Match 2
            search_words.append(match_on_date[1])
            run(search_words,date_since)
            search_words.remove(match_on_date[1])
        else:
            print("Will start at 3:30 IST")

    elif hour >= 14:
        #Match 1
        search_words.append(match_on_date)
        print(search_words)
        run(search_words,date_since)
    else:
        print("Will start at 7:30 IST")