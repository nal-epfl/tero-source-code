import boto3
import os
import redis

from logger import get_logger
from config import rw_access_key, rw_secret_key, s3_url, bucket_name, stream_files_path, redis_host, redis_port, redis_password


if __name__ == "__main__":
    logger = get_logger("upload_stream_files")
    cache = redis.Redis(host=redis_host, port=redis_port, password=redis_password)

    logger.info("Starting process...")

    client = boto3.client(
        's3',
        aws_access_key_id=rw_access_key,
        aws_secret_access_key=rw_secret_key,
        endpoint_url=s3_url
    )

    for stream_file in os.listdir(stream_files_path):                    
        ob = client.list_objects(Bucket=bucket_name, Prefix="streams/{}".format(stream_file))

        if "Contents" not in ob:
            logger.info("Found file to upload: {}".format(stream_file))
            client.upload_file(os.path.join(stream_files_path, stream_file), bucket_name, "streams/{}".format(stream_file))
            cache.sadd("stream_files", stream_file)

    # Check if the files are in remote before deleting them
    for stream_file in os.listdir(stream_files_path):
        ob = client.list_objects(Bucket=bucket_name, Prefix="streams/{}".format(stream_file))

        if "Contents" in ob:
            logger.info("Deleting file: {}".format(stream_file))
            os.remove(os.path.join(stream_files_path, stream_file))

    logger.info("Finished process...")