import time
import tarfile
import subprocess
import sys
import pandas as pd
import pandas_gbq
import numpy as np
import mysql.connector
from decouple import config
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage
from py.bucket import download_from_bucket, delete_from_bucket, extract_tarfile
from py.sqldocker import mount_sql_container, dump_container_sql
from py.aobq import time_measure, queries_to_dict

BUCKET_NAME=config('BUCKET_NAME')
BLOB_NAME=config('BLOB_NAME')
KEY_CREDENTIALS=config('KEY_CREDENTIALS')
PATH_SQL=config('PATH_DUMP')
SQL_FILE=config('FILE_SQL')
DESTINATION_FILENAME=f"{config('PATH_DUMP')}/{config('FILE_DUMP')}"
DOCKER_NAME_SQL=config('DOCKER_NAME_SQL')
DOCKER_DATABASE=config('DOCKER_DATABASE')
DOCKER_DB_PORT=config('DOCKER_DB_PORT')
FILEPATHSQL=f"{config('PATH_DUMP')}/{config('FILE_SQL')}"
DOCKER_PASS=config('DOCKER_PASS')
FILE_SQL_BQ=config('FILE_SQL_BQ')
ID_PROJECT_DA=config('ID_PROJECT_DA')
KEY_CREDENTIALS=config('KEY_CREDENTIALS')
DATASET_BQ=config('DATASET_BQ')


try:
    global_init=time.time()
    time_init=time.time()
    download_from_bucket(BUCKET_NAME,BLOB_NAME,KEY_CREDENTIALS,DESTINATION_FILENAME)
    extract_tarfile(DESTINATION_FILENAME,SQL_FILE,PATH_SQL)
    delete_from_bucket(BUCKET_NAME,BLOB_NAME,KEY_CREDENTIALS)
    print(f"Elapsed time for downloading {time_measure(time_init)}")
    time_init=time.time()
    mount_sql_container(DOCKER_NAME_SQL,DOCKER_DATABASE,DOCKER_PASS,DOCKER_DB_PORT)
    dump_container_sql(FILEPATHSQL,DOCKER_NAME_SQL,DOCKER_PASS,DOCKER_DATABASE)
    print(f"Elapsed time for mounting container {time_measure(time_init)}")
 # Init connection
    initial_start_time = time.time()
    cnx = mysql.connector.connect(user='root', password=DOCKER_PASS, host='localhost', database=DOCKER_DATABASE,charset='utf8',port=DOCKER_DB_PORT)

# Queries
    queries=queries_to_dict(FILE_SQL_BQ)

 # BQ credentials
    credentials = service_account.Credentials.from_service_account_file(KEY_CREDENTIALS,)

# Extract and load
    for sql in queries.keys():
        start_time = time.time()
        try:
            print(f"Processing:{queries[sql]['table']}")
            start_time_process = time.time()
            tmp=pd.read_sql(queries[sql]['query'], con=cnx).convert_dtypes(convert_boolean=False)
            print(f" ---- Elapsed time for extraction and processing {time_measure(start_time_process)} ---")        
            start_time_upload = time.time()
            tmp.to_gbq(f"{DATASET_BQ}.{queries[sql]['table']}",project_id=ID_PROJECT_DA,credentials=credentials,if_exists='replace')       
            print(f"--- Elapsed time for loading to BQ {time_measure(start_time_upload)} ---")
            print('ETL process finished for:{}'.format(queries[sql]['table']))
        except Exception as e:
            print("Oops!", e.__class__, "occurred in {}".format(queries[sql]['table']))
            print(e)
            print('Next entry \n\n')
        print(f" ETL process finished for {queries[sql]['table']}. Elapsed time {time_measure(start_time)} ")
    print(f"--- Elapsed time for loading {time_measure(initial_start_time)}  ---")
    cnx.close()
    print(f"--- Elapsed global time {time_measure(global_init)}  ---")
except:
    print('Error in process')



