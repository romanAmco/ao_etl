import sys
import pandas as pd
import pandas_gbq
import numpy as np
import mysql.connector
from google.cloud import bigquery
from google.oauth2 import service_account
import time
from decouple import config

# Meassuring execution time

def time_measure(initial_time):
    total_time=time.time()-initial_time
    return(f"{total_time//60}h {total_time%60}m")

def queries_to_dict(FILE_SQL_BQ):
    f=open(FILE_SQL_BQ,'r')
    sqls=f.read().replace('\n','').split(';')
    queries={}
    for sql in sqls:
        try:
            ii=sql.split('@')
            queries[ii[0]]={'table':ii[0],'query':ii[1]}
        except:
            print('Query no pudo ser procesado')
            print(sql)
    f.close()
    return queries


if __name__ == "__main__":

    DOCKER_PASS=config('DOCKER_PASS')
    DOCKER_DATABASE=config('DOCKER_DATABASE')
    DOCKER_DB_PORT=config('DOCKER_DB_PORT')
    FILE_SQL_BQ=config('FILE_SQL_BQ')
    ID_PROJECT_DA=config('ID_PROJECT_DA')
    KEY_CREDENTIALS=config('KEY_CREDENTIALS')
    DATASET_BQ=config('DATASET_BQ')


    # Init connection
    initial_start_time = time.time()
    cnx = mysql.connector.connect(user='root', password=DOCKER_PASS, host='localhost', database=DOCKER_DATABASE,charset='utf8',port=config('DOCKER_DB_PORT'))


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


    print(f"--- Elapsed time {time_measure(initial_start_time)}  ---")
    cnx.close()

