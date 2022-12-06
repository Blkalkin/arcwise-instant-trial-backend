import pyarrow.parquet as pq
from fastapi import FastAPI
import duckdb
import fastapi
# Imports the Google Cloud client library
from google.cloud import storage


app = FastAPI()
storage_client = storage.Client()
con = duckdb.connect()

bucket_name = 'arcwise-instant-trial-storage'
bucket = storage_client.bucket(bucket_name)
file_path = '/Users/balaji/Downloads/baseballdatabank-2022.2/core/AllstarFull.csv'

def upload_to_gcs_bucket(name, file_path, bucket):
    try:
        blob = bucket.blob(name)
        blob.upload_from_filename(file_path)
        return True
    except Exception as e:
        print(e)
        return False

def csv_to_parquet(file_path):
    con.execute("COPY (SELECT * FROM read_csv_auto('/Users/balaji/Downloads/baseballdatabank-2022.2/core/AllstarFull.csv')) TO 'result.parquet' (FORMAT 'parquet')")
    #con.execute('').fetchall())

if __name__ == '__main__':
    csv_to_parquet(file_path)
    upload_to_gcs_bucket('result','result.parquet',bucket)
