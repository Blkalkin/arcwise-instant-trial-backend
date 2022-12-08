from fastapi import FastAPI, File, Form, UploadFile
import duckdb
from model import SqlQuery
# Imports the Google Cloud client library
from google.cloud import storage
import os


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

@app.post("/upload")
def upload(file: UploadFile = File(...)):
    try:
        contents = file.file.read()
        with open(file.filename, 'wb') as f:
            f.write(contents)
        f_name_truncated = file.filename[:-4]
        csv_to_parquet(f_name_truncated, file.filename)
        upload_to_gcs_bucket(f_name_truncated, f'{f_name_truncated}.parquet', bucket)
    except Exception:
        return {"message": "There was an error uploading the file"}
    finally:
        os.remove(file.filename)
        os.remove(f'{f_name_truncated}.parquet')
        file.file.close()
    return {"message": "File uploaded"}

@app.post("/query/")
async def execute_query_gcs(query: SqlQuery):
    return con.execute(f"{query.sql_string}").fetchall()

def upload_to_gcs_bucket(name, file_path, bucket):
    try:
        blob = bucket.blob(name)
        blob.upload_from_filename(file_path)
        return True
    except Exception as e:
        print(e)
        return False

def csv_to_parquet(file_name, file_path):
    con.execute(f"COPY (SELECT * FROM read_csv_auto('{file_path}')) TO '{file_name}.parquet' (FORMAT 'parquet')")


def test_gcs():
    return con.execute("select * from read_parquet('s3://arcwise-instant-trial-storage/result')").fetchall()

