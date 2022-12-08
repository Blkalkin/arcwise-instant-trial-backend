from fastapi import FastAPI, File, Form, UploadFile
import duckdb
# Imports the Google Cloud client library
from google.cloud import storage


app = FastAPI()
storage_client = storage.Client()
con = duckdb.connect()
con.execute('INSTALL httpfs;')
con.execute('LOAD httpfs;')
con.execute("SET s3_endpoint='storage.googleapis.com';")
con.execute("SET s3_access_key_id='GOOG7KVGGKIVTAV3ROV4GOWN';")
con.execute("SET s3_secret_access_key='Q7nZGfWCJZP3dKp0voMmkuIV41wkxb18Bv3xU67p';")

bucket_name = 'arcwise-instant-trial-storage'
bucket = storage_client.bucket(bucket_name)


def upload_to_gcs_bucket(name, file_path, bucket):
    try:
        blob = bucket.blob(name)
        blob.upload_from_filename(file_path)
        return True
    except Exception as e:
        print(e)
        return False

def csv_to_parquet(file_path):
    con.execute(f"COPY (SELECT * FROM read_csv_auto('{file_path}')) TO 'result.parquet' (FORMAT 'parquet')")


def test_gcs():
    print(con.execute("select * from read_parquet('s3://arcwise-instant-trial-storage/result')").fetchall())


if __name__ == '__main__':
    test_gcs()
    #csv_to_parquet(file_path)
    #upload_to_gcs_bucket('result','result.parquet',bucket)
