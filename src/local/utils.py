import tweepy as tw
import re
import pandas as pd
import nltk
from google.cloud import bigquery
from google.cloud import storage
from nltk.sentiment.vader import SentimentIntensityAnalyzer

nltk.download('vader_lexicon')

def getTweets(consumer_key,consumer_secret,access_token,access_token_secret,searchkey,date,maxTweetId):

    auth = tw.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tw.API(auth, wait_on_rate_limit=True)

    # Define the search term and the date_since date as variables
    search_words = searchkey
    date_since = date
    # Collect tweets
    tweets = tw.Cursor(api.search_tweets,
                       q=search_words,
                       lang="en",
                       since=date_since,tweet_mode="extended",since_id=maxTweetId).items(1000)

    tweet_details = [[tweet.id,tweet.created_at, tweet.full_text,tweet.user.screen_name,tweet.user.location] for tweet in tweets]
    tweet_df = pd.DataFrame(data=tweet_details, columns=['id','created_at','text','user_screen_name','user_location'])
    pd.set_option('max_colwidth', 800)

    # Clean tweets
    tweet_df['text'] = tweet_df['text'].apply(lambda x: clean_tweets(x))
    return tweet_df

def clean_tweets(text):
  text = re.sub("RT @[\w]*:","",text)
  text = re.sub("@[\w]*","",text)
  text = re.sub("https?://[A-Za-z0-9./]*","",text)
  text = re.sub("\n","",text)
  return text

def getPolarityScores(tweet_df):
    tweet_df_sentiment = tweet_df
    sid = SentimentIntensityAnalyzer()
    tweet_df_sentiment['polarity_scores'] = tweet_df['text'].apply(lambda x:sid.polarity_scores(x))
    return tweet_df_sentiment

def getSentimentValue(x):
    sentiment = ""
    if x['compound'] >= 0.05:
        sentiment = "Positive"
    elif x['compound'] > -0.05 and x['compound'] < 0.05:
        sentiment = "Neutral"
    else:
        sentiment = "Negative"
    return sentiment