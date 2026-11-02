import boto3
import redis
import json

from dateutil import parser
from datetime import datetime

from config import s3_url, rw_access_key, rw_secret_key, bucket_name

client = boto3.client(
    's3',
    aws_access_key_id=rw_access_key,
    aws_secret_access_key=rw_secret_key,
    endpoint_url=s3_url
)

file_name = "dumps/mongo_dumps.zip"

client.download_file(bucket_name, file_name, "backups.zip")