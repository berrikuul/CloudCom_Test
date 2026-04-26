import traceback
import boto3
from botocore.exceptions import NoCredentialsError
from airflow.hooks.base import BaseHook
import logging



def get_s3_client():
    try:
        conn = BaseHook.get_connection("minio_conn")
        s3 = boto3.client(
            's3',
            endpoint_url=conn.extra_dejson.get('endpoint_url'),
            aws_access_key_id=conn.login,
            aws_secret_access_key=conn.password,
            region_name='us-east-1'
        )
        return s3
    except NoCredentialsError:
        logging.error('Credentials not avaliable')
    except Exception as e:
        logging.error(f"Exception occurred while creating S3 client!\n{e}")
        traceback.print_exc()

def create_minio_bucket(bucket_name):
    try:
        s3 = get_s3_client()

        response = s3.list_buckets()
        existing_buckets = [bucket['Name'] for bucket in response.get('Buckets',[])]

        if bucket_name in existing_buckets:
            logging.info(f'Bucket {bucket_name} already exists!')
        else:
            s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': 'us-east-1'})
            logging.info(f'Bucket {bucket_name} created!')
    except NoCredentialsError:
        logging.error('Credentials not avaliable')

def upload_to_s3(file_path, bucket_name, object_name):
    try:
        s3 = get_s3_client()

        s3.upload_file(file_path, bucket_name, object_name)
        logging.info(f'File {object_name} uploaded to S3!')
    except FileNotFoundError:
        logging.error("The file was not found")
    except NoCredentialsError:
        logging.error("Credentials not avaliable")


