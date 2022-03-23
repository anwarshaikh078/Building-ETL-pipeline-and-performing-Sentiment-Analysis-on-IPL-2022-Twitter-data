from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
import subprocess
from datetime import datetime

def WriteToGCS(data):
    today = datetime.now()
    filename = "Tweets_" + str(today).split('.')[0].replace(":","_").replace("-","_").replace(" ","_") + ".csv" 
    partition = str(today).split(' ')[0].replace("-","")
    destination_uri = 'gs://twitter-archieve/' + partition +'/'+filename
    data.to_csv(destination_uri,index=False,columns=["id","created_at","text","user_screen_name","user_location","sentiment","compound"])
    return destination_uri

def WriteToBQ(uri):
    # dataset_name = "twitter_ipl_dataset"
    # table_name = "twitterIPLData2022"
    # cmd = 'bq load --autodetect --skip_leading_rows=1 --max_bad_records=50 --time_partitioning_field=created_at --source_format=CSV '+dataset_name+'.'+table_name+' '+uri
    # print(str(cmd))
    # subprocess.call(cmd,shell=True)

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    # table_id = "your-project.your_dataset.your_table_name"

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("id", "INTEGER"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("text", "STRING"),
            bigquery.SchemaField("user_screen_name", "STRING"),
            bigquery.SchemaField("user_location", "STRING"),
            bigquery.SchemaField("sentiment", "STRING"),
            bigquery.SchemaField("compound", "FLOAT"),
        ],
        skip_leading_rows=1,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="created_at",
            expiration_ms=7776000000,  # 90 days.
        ),
    )
    table_id = 'tangential-leaf-344613:twitter_ipl_dataset.twitterIPLData2022'
    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)
    print("Loaded {} rows to table {}".format(table.num_rows, table_id))