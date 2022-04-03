from google.cloud import bigquery
from google.cloud import storage
from datetime import datetime
import pandas as pd
import subprocess
import os

bucket_name = os.environ.get('bucket_name', 'Specified environment variable is not set.')
dataset_name = os.environ.get('dataset_name', 'Specified environment variable is not set.')
table_name = os.environ.get('table_name', 'Specified environment variable is not set.')
project_name = os.environ.get('project_name','Specified environment variable is not set.')

def WriteToGCS(data):
    today = datetime.now()
    filename = "Tweets_" + str(today).split('.')[0].replace(":","_").replace("-","_").replace(" ","_") + ".csv" 
    partition = str(today).split(' ')[0].replace("-","")
    destination_uri = 'gs://'+bucket_name+ '/' + partition +'/'+filename
    data.to_csv(destination_uri,index=False,columns=["id","created_at","text","user_screen_name","user_location","sentiment","compound","match"])
    return destination_uri

def WriteToBQ(uri):
   
    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("id", "INTEGER"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("text", "STRING"),
            bigquery.SchemaField("user_screen_name", "STRING"),
            bigquery.SchemaField("user_location", "STRING"),
            bigquery.SchemaField("sentiment", "STRING"),
            bigquery.SchemaField("compound", "FLOAT"),
            bigquery.SchemaField("match", "STRING"),
        ],
        skip_leading_rows=1,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="created_at",
            expiration_ms=7776000000,  # 90 days.
        ),
        max_bad_records=100,
    )
    table_id = dataset_name+"."+table_name
    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)
    print("Loaded {} rows to table {}".format(table.num_rows, table_id))


def getMaxTweetId(date,match):

    maxTweetID = 99999999999999
    client = bigquery.Client()

    query = """select max(id) from `"""+project_name+"""."""+dataset_name+"""."""+table_name+"""` where DATE(created_at) = '"""+date+"""' and match='"""+match+"'"
    query_job = client.query(query)

    for row in query_job:
        print("MaxTweetID={}".format(row[0]))
        maxTweetID = row[0]

    return int(maxTweetID)

def insertMaxTweetId(date,match,maxId):
    client = bigquery.Client()
    query = """INSERT INTO `"""+project_name+"""."""+dataset_name+"""."""+table_name+"""` VALUES('"""+date+"""', '"""+match+"""', """+str(maxId)+""")"""
    query_job = client.query(query)
    if query_job.done() == True:
        print("Inserted MaxTweetId in CDC table")
    else:
        print("Error Loading MaxTweetId")