# Building ETL pipeline and performing Sentiment Analysis on IPL 2022 Twitter data

Use Case: -  The user wants to capture IPL 2022 tweets at a rate of 1000 every 5min. Just so to get an understanding of how the APIs work, and later load and persist this data to a table to perform some analysis and Visualization on top of it. While loading the data he also wants to give sentiment scores to each tweet, if it is positive, negative, or neutral tweet.

Proposed Solution: - As you can see in the picture below, we will be using multiple components of the Google Cloud Platform. The solution involves using a cloud scheduler, which publishes messages to cloud pubsub every 5min in the given window. The pubsub receives these messages and triggers the cloud functions. The cloud function is the principal component in use, it will connect to Twitter API, bring tweets, clean them, and give polarity scores to identify the sentiment for the tweet. Once all this is done, it will store the data in google cloud storage and load the updated data frame to a big query. Data Studio, will connect with bigquery and give you the visualizations.

![Architecture](/architecture/architecture.jpg "Image Title")

<br>

Read the full article on [Medium][identifier]

[identifier]: https://anwarshaikh078.medium.com/building-etl-pipeline-and-performing-sentiment-analysis-on-ipl-2022-twitter-data-f488e038d66a
