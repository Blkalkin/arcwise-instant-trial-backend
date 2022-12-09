from fastapi import FastAPI, File, UploadFile, Form, status
from fastapi.exceptions import HTTPException
import aiofiles
import duckdb
from model import SqlQuery
# Imports the Google Cloud client library
from google.cloud import storage
import os

app = FastAPI()
storage_client = storage.Client()
con = duckdb.connect("::memory::")

#loading GCS
con.execute('INSTALL httpfs;')
con.execute('LOAD httpfs;')
con.execute("SET s3_endpoint='storage.googleapis.com';")
con.execute("SET s3_access_key_id='GOOG7KVGGKIVTAV3ROV4GOWN';")
con.execute("SET s3_secret_access_key='Q7nZGfWCJZP3dKp0voMmkuIV41wkxb18Bv3xU67p';")

#fixed bucket
bucket_name = 'arcwise-instant-trial-storage'
bucket = storage_client.bucket(bucket_name)

CHUNK_SIZE = 1024 * 1024
@app.post("/uploadtest")
async def upload(file: UploadFile = File(...)):
    try:
        filepath = os.path.join('./', os.path.basename(file.filename))
        async with aiofiles.open(filepath, 'wb') as f:
            while chunk := await file.read(CHUNK_SIZE):
                await f.write(chunk)
        f_name_truncated = file.filename[:-4]
        csv_to_parquet(f_name_truncated, file.filename)
        upload_to_gcs_bucket(f_name_truncated, f'{f_name_truncated}.parquet', bucket)
    except Exception:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail='There was an error uploading the file')
    finally:
        os.remove(file.filename)
        os.remove(f'{f_name_truncated}.parquet')
        await file.close()

    return {"message": f"Successfuly uploaded {file.filename}"}

@app.post("/query/")
async def execute_query_gcs(query: SqlQuery):
    sub_query = query.sql_string.replace(f"{query.file_name}.csv",f"read_parquet('s3://arcwise-instant-trial-storage/{query.file_name}')")
    return con.execute(f"{sub_query}").fetchall()

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



