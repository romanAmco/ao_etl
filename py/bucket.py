from decouple import config
from google.cloud import storage
import tarfile


def download_from_bucket(BUCKET_NAME,BLOB_NAME,KEY_CREDENTIALS,DESTINATION_FILENAME):
    client = storage.Client.from_service_account_json(KEY_CREDENTIALS)
    try:
        client.bucket(BUCKET_NAME).blob(BLOB_NAME).download_to_filename(DESTINATION_FILENAME)
        print(f"{BLOB_NAME} downloaded succesfully")
    except:
        print('Error downloading file')


def delete_from_bucket(BUCKET_NAME,BLOB_NAME,KEY_CREDENTIALS):
    client = storage.Client.from_service_account_json(KEY_CREDENTIALS)
    try:
        client.bucket(BUCKET_NAME).blob(BLOB_NAME).delete()
        print(f"{BLOB_NAME} deleted succesfully")
    except:
        print('Error deleting  file')


def extract_tarfile(FILENAME,SQL_FILE,PATH_SQL):
    try:
        my_tar = tarfile.open(FILENAME)
        my_tar.extract(SQL_FILE,PATH_SQL)
        my_tar.close()
    except:
        print('Error extracting file')


if __name__ == "__main__":
    BUCKET_NAME=config('BUCKET_NAME')
    BLOB_NAME=config('BLOB_NAME')
    KEY_CREDENTIALS=config('KEY_CREDENTIALS')
    PATH_SQL=config('PATH_DUMP')
    SQL_FILE=config('FILE_SQL')
    DESTINATION_FILENAME=f"{config('PATH_DUMP')}/{config('FILE_DUMP')}"
    try:
        download_from_bucket(BUCKET_NAME,BLOB_NAME,KEY_CREDENTIALS,DESTINATION_FILENAME)
        extract_tarfile(DESTINATION_FILENAME,SQL_FILE,PATH_SQL)
        delete_from_bucket(BUCKET_NAME,BLOB_NAME,KEY_CREDENTIALS)
    except:
        print('Error in process')
