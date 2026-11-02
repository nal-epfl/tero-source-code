import redis
import os
import json
import boto3

from zipfile import ZipFile, BadZipfile
from config import redis_host, redis_password, to_process_storage, s3_url, rw_access_key, rw_secret_key, bucket_name, img_storage
from logger import get_logger


if __name__ == "__main__":
    client = boto3.client(
        's3',
        aws_access_key_id=rw_access_key,
        aws_secret_access_key=rw_secret_key,
        endpoint_url=s3_url
    )

    logger = get_logger("complete_batches")

    logger.info("Starting process...")
    cache = redis.Redis(host=redis_host, port=6379, password=redis_password)

    not_listed = {}

    for batch_name, batch_data in cache.hgetall("currently_processing").items():
        name = batch_name.decode("utf8")
        
        if not os.path.isfile("{}/{}.txt".format(to_process_storage, name)):
            not_listed[name] = json.loads(batch_data.decode("utf8"))
    
    print(not_listed)

    for batch_name, batch_data in not_listed.items():
        for zip_name in batch_data["contents"]:
            ob = client.list_objects(Bucket=bucket_name, Prefix="{}".format(zip_name))
    
            if "Contents" in ob:
                for key in ob['Contents']:
                    client.download_file(bucket_name, key["Key"], key["Key"])

            with open("{}.txt".format(batch_name), "w+") as f:
                for zip_name in os.listdir("bw"):
                    with ZipFile("bw/{}".format(zip_name), 'r') as zip_ref:
                        zip_ref.extractall(img_storage)
                        
                        for l in zip_ref.namelist():
                            f.write("{}\n".format(l))

                for zip_name in os.listdir("bw"):  
                    os.remove("bw/{}".format(zip_name))    
                        
        