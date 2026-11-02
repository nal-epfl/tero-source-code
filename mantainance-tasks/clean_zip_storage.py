import os
import boto3
import redis
import json

from pymongo import MongoClient

from logger import get_logger

from config import redis_host, redis_password, redis_port, rw_access_key, rw_secret_key, s3_url, mongo_host, mongo_user, mongo_password, bucket_name

zip_storage_path = "/raid1/image-processing/zip-storage"


if __name__ == "__main__":
    logger = get_logger("clean_zip_storage")

    client = boto3.client(
        's3',
        aws_access_key_id=rw_access_key,
        aws_secret_access_key=rw_secret_key,
        endpoint_url=s3_url
    )

    cache = redis.Redis(host=redis_host, port=redis_port, db=0, password=redis_password)
    batches_processed = set()
 
    logger.info("Gathering processed batches")
    to_postprocess = cache.smembers("to_postprocess")
    
    for s in to_postprocess:
        data = json.loads(s.decode())
        for c in data["contents"]:
            batches_processed.add(c)
        
    currently_processing = cache.hgetall("currently_processing")
    for _, s in currently_processing.items():
        data = json.loads(s.decode())
        for c in data["contents"]:
            batches_processed.add(c)

    mongo_client = MongoClient("mongodb://{}:27017/".format(mongo_host),
                            username=mongo_user,
                            password=mongo_password)

    processed = mongo_client.metadata.batches.find({})
    for p in processed:
        for c in p["contents"]:
            batches_processed.add(c)

    logger.info("\t Found {} processed batches".format(len(batches_processed)))

    logger.info("Gathering local batches")
    bw_to_check = []
    tiny_to_check = []

    for file_name in os.listdir(zip_storage_path):
        if "bw.zip" in file_name:
            bw_to_check.append(file_name)

        if "tiny.zip" in file_name:
            tiny_to_check.append(file_name)
    
    logger.info("\t Found: {} tiny, {} BW".format(len(bw_to_check), len(tiny_to_check)))

    logger.info("Compiling to-upload/to-delete")

    to_delete = set()
    bw_to_upload = set()

    for tu in bw_to_check:
        path_to_zip = "bw/{}".format(tu)
        if not cache.sismember("zip_to_process", path_to_zip) and path_to_zip not in batches_processed:
            bw_to_upload.add(tu)
        else:
            to_delete.add(tu)

    tiny_to_upload = set()
    for tc in tiny_to_check:
        try:
            result = client.head_object(Bucket=bucket_name, Key="tiny/{}".format(tc))
            to_delete.add(tc)
        except Exception as e:
            try:
                if e.response['Error']['Code'] == "404":
                    tiny_to_upload.add(tc)
            except Exception as e:
                tiny_to_upload.add(tc)
         
    logger.info("Deleting. Total to delete: {}".format(to_delete))
    for td in to_delete:
        if os.path.isfile("{}/{}".format(zip_storage_path, td)):
            os.remove("{}/{}".format(zip_storage_path, td))

    logger.info("Uploading tiny. Total to upload: {}".format(len(tiny_to_upload)))
    for file_to_upload in tiny_to_upload:
        try:
            client.upload_file("{}/{}".format(zip_storage_path, file_to_upload), bucket_name, "tiny/{}".format(file_to_upload))        
        except Exception as e:
            # If something fails while uploading, skip the zip
            logger.info("Could not upload {}".format(file_to_upload))

    logger.info("Uploading BW. Total to upload: {}".format(len(bw_to_upload)))
    for file_to_upload in bw_to_upload:
        try:
            client.upload_file("{}/{}".format(zip_storage_path, file_to_upload), bucket_name, "bw/{}".format(file_to_upload))      
            cache.sadd("zip_to_process", "bw/{}".format(file_to_upload))  
        except Exception as e:
            # If something fails while uploading, skip the zip
            logger.info("Could not upload {}".format(file_to_upload))