import redis
import boto3
import os
from dateutil import tz
from datetime import datetime, timedelta

from config import bucket_name, rw_access_key, rw_secret_key, s3_url, redis_host, redis_port, redis_password
from logger import get_logger


if __name__ == "__main__":
    logger = get_logger("backup_redis")

    client = boto3.client(
        's3',
        aws_access_key_id=rw_access_key,
        aws_secret_access_key=rw_secret_key,
        endpoint_url=s3_url
    )

    now = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')

    cache = redis.Redis(host=redis_host, port=redis_port, db=0, password=redis_password)
    users_to_probe = [x.decode("utf-8") for x in cache.smembers("to_probe")]

    logger.info("Users to backup: {}".format(len(users_to_probe)))

    backup_file = "redis-to-probe-{}.json".format(now)

    with open(backup_file, "w+") as f:
        for u in users_to_probe:
            f.write("{}\n".format(u))

    # Keep backups for a week, delete everything that is older:
    objects = client.list_objects(Bucket=bucket_name, Prefix="backups/redis-to-probe")
    if "Contents" in objects:
        for idx, ob in enumerate(objects["Contents"]):
            if ob["LastModified"] < datetime.now(tz=tz.tzutc()) - timedelta(days=8):
                client.delete_object(Bucket=bucket_name, Key=ob["Key"])
                logger.info("Deleting: {}".format(ob["Key"]))

    logger.info("Uploading: {}".format(backup_file))
    client.upload_file(backup_file, bucket_name, "backups/{}".format(backup_file))

    if os.path.isfile(backup_file):
        os.remove(backup_file)
