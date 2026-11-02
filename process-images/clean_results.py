import os
import boto3
import redis

from config import s3_url, rw_access_key, rw_secret_key, bucket_name, redis_host, redis_password, redis_port, results_storage
from logger import get_logger


if __name__ == "__main__":
    logger = get_logger("clean_results")

    logger.info("Starting process...")

    client = boto3.client(
        's3',
        aws_access_key_id=rw_access_key,
        aws_secret_access_key=rw_secret_key,
        endpoint_url=s3_url
    )

    cache = redis.Redis(host=redis_host, port=redis_port, db=0, password=redis_password)

    to_check = set()
    for f in os.listdir(results_storage):
        if "log" not in f:
            to_check.add(f)

    logger.info("Found {} entries to check".format(len(to_check)))

    to_delete = set()
    not_in_remote = set()
    for tc in to_check:
        ob = client.list_objects(Bucket=bucket_name, Prefix="raw_latency/{}".format(tc))

        if "Contents" in ob:
            to_delete.add(tc)

    logger.info("Found {} entries to delete".format(len(to_delete)))
        
    for td in to_delete:
        file_name = "{}/{}".format(results_storage, td)
        associated_log = "{}/{}".format(results_storage, td.replace(".json", ".txt").replace("matches", "log"))

        logger.info("Deleted {}".format(file_name))
        if os.path.isfile(file_name):
            os.remove(file_name)

        logger.info("Deleted {}".format(associated_log))
        if os.path.isfile(associated_log):
            os.remove(associated_log)
        
    logger.info("Finished process...")