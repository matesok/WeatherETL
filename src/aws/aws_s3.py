import logging
import os

import boto3
from botocore.exceptions import ClientError

_client = boto3.client(
    's3',
    aws_access_key_id='test',
    aws_secret_access_key='test',
    endpoint_url=os.getenv('LOCALSTACK_ENDPOINT_URL', 'http://localhost:4566'),
)
_bucket = os.getenv('AWS_WEATHER_BUCKET_NAME', 'bucket')


def upload_file(buffer, stage, bucket_dir, file_name):
    key = f'{stage}/{bucket_dir}/{file_name}'
    try:
        _client.upload_fileobj(
            buffer,
            Bucket=_bucket,
            Key=key
        )
        return key
    except ClientError as e:
        logging.error(e)


def list_files(bucket_dir):
    objects = _client.list_objects_v2(
        Bucket=_bucket,
        Prefix=f'{bucket_dir}/'
    )

    return [x['Key'] for x in objects.get('Contents', [])]


def fetch_file(key):
    try:
        response = _client.get_object(
            Bucket=_bucket,
            Key=key,
        )
        return response['Body'].read()
    except ClientError as e:
        return None
