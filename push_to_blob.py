from scrape import *
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import boto3
import configparser
# from dotenv import load_dotenv
import os

# load_dotenv()

functions = [league_table, top_scorers, detail_top, player_table,
             all_time_table, all_time_winner_club, top_scorers_seasons, goals_per_season]


def to_blob(func):
    try:
        print("Started")
        file_name = func.__name__
        df = func()  # Get the DataFrame result from the function

        if not isinstance(df, pd.DataFrame):
            raise ValueError(f"Expected a DataFrame, got {type(df)}")

        print(f"File name is: {file_name}")

        # Convert DataFrame to Arrow Table
        table = pa.Table.from_pandas(df)

        # Write table to Parquet format
        parquet_buffer = pa.BufferOutputStream()
        pq.write_table(table, parquet_buffer)

        s3_file = f"{file_name}.parquet"

        print("Getting AWS credentials")
        session = boto3.Session(profile_name='srinivasvkumar')
        credentials = session.get_credentials()

        print("Credentials obtained")
        print(f"AWS_ACCESS_KEY_ID = {credentials.access_key}")
        print(f"AWS_SECRET_ACCESS_KEY = {credentials.secret_key}")
        print(f"AWS_SESSION_TOKEN = {credentials.token}")

        s3 = session.client('s3')

        # Upload the Parquet data directly from the buffer
       # s3.upload_fileobj(pa.BufferReader(parquet_buffer.getvalue()), 'srinivasepl', s3_file)
        print(f"File uploaded to S3: {s3_file}")

    except Exception as e:
        print(f"An error occurred: {e}")


# Assuming 'functions' is a list of functions returning DataFrames
for func in functions:
    to_blob(func)
