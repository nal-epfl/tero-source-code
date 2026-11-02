import boto3
import redis
import os
import sys
import json
 
from pymongo import MongoClient
from config import rw_access_key, rw_secret_key, bucket_name, redis_host, redis_password, mongo_host, mongo_user, mongo_password, paths_to_check
from logger import get_logger
 

if __name__ == "__main__":
   logger = get_logger("upload-failed-zips")

   logger.info("Started processing...")
   to_check = []
   
   for path in paths_to_check:
      for x in os.listdir(path):
         if ".zip" in x:
            to_check.append("{}/{}".format(path, x))
   
   if not to_check:
      logger.info("No zips to check, ending process")
      sys.exit(0)

   cache = redis.Redis(host=redis_host, port=6379, password=redis_password)

   client = boto3.client(
      's3',
      aws_access_key_id=rw_access_key,
      aws_secret_access_key=rw_secret_key,
      endpoint_url="https://s3.epfl.ch"
   )

   batches_processed = set()

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

   to_delete = []
   bw_upload = [x for x in to_check if "bw.zip" in x]
   bw_to_upload = []

   for tu in bw_upload:
      path_to_zip = "bw/{}".format(tu.split("/")[-1])
      if not cache.sismember("zip_to_process", path_to_zip) and path_to_zip not in batches_processed:
         bw_to_upload.append(tu)     
      else:
         to_delete.append(tu)

   logger.info("Found {}/{} bw to upload".format(len(bw_to_upload), len(bw_upload)))

   tiny_upload = [x for x in to_check if "tiny.zip" in x]
   tiny_to_upload = []

   for tc in tiny_upload:
      try:
         result = client.head_object(Bucket=bucket_name, Key="tiny/{}".format(tc.split("/")[-1]))
         to_delete.append(tc)
      except Exception as e:
         try:
            if e.response['Error']['Code'] == "404":
               tiny_to_upload.append(tc)
         except Exception as e:
            print(e)
            tiny_to_upload.append(tc)
      
   logger.info("Found {}/{} tiny to upload".format(len(tiny_to_upload), len(tiny_upload)))

   for tu in bw_to_upload:
      path_to_zip = "bw/{}".format(tu.split("/")[-1])
      
      try:
         client.upload_file(tu, bucket_name, path_to_zip)
         cache.sadd("zip_to_process", path_to_zip)
      
         if os.path.isfile(tu):
            os.remove(tu)
      except Exception as e:
         logger.info("Could not upload {}. Error: {}".format(tu, e))


   for tu in tiny_to_upload:
      path_to_zip = "tiny/{}".format(tu.split("/")[-1])

      try:
         client.upload_file(tu, bucket_name, path_to_zip)
         if os.path.isfile(tu):
            os.remove(tu)
      except Exception as e:
         logger.info("Could not upload {}. Error: {}".format(tu, e))

   logger.info("Found {} to delete".format(len(to_delete)))
   
   for td in to_delete:
      if os.path.isfile(td):
         os.remove(td)
   
   logger.info("Finished processing...")
