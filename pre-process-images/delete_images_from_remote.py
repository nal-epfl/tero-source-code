import boto3
import redis
import sys

from botocore.client import Config
from logger import get_logger
from config import batch_size, s3_url, rw_access_key, rw_secret_key, bucket_name, increase_batch_threshold, redis_host, redis_password


if __name__ == '__main__':
    idx = 0
    if len(sys.argv) > 1:
        idx = sys.argv[1]

    logger = get_logger("delete_from-remote-{}".format(idx))

    cache = redis.Redis(host=redis_host, port=6379, password=redis_password)

    s3_client = boto3.client(
        's3',
        aws_access_key_id=rw_access_key,
        aws_secret_access_key=rw_secret_key,
        endpoint_url=s3_url,
        config=Config(connect_timeout=5, retries={'max_attempts': 0})
    )

    images_to_delete = [x.decode("utf8") for x in cache.spop("raw_images_to_delete", count=100000)]

    deleted_images = []
    for idx, image in enumerate(images_to_delete):
        if idx % 1000 == 0:
            logger.info("Deleting: {}/{}".format(idx, len(images_to_delete)))
        
        try:
            s3_client.delete_object(Bucket=bucket_name, Key=image)
            deleted_images.append(image)
        except Exception as e:
            logger.info("Failed deleting {}: {}".format(image, e))
                        
    logger.info("Deleted {}/{} images from S3.".format(len(deleted_images), len(images_to_delete)))
