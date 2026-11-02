import os
import json
import redis
import shutil
import boto3
import re

from datetime import datetime
from pymongo import MongoClient
from config import redis_host, redis_password, bucket_name, mongo_host, mongo_user, mongo_password, rw_access_key, rw_secret_key, s3_url


class OnlineController:
    def __init__(self, logger):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=rw_access_key,
            aws_secret_access_key=rw_secret_key,
            endpoint_url=s3_url
        )
        self.mongo_client = MongoClient("mongodb://{}:27017/".format(mongo_host),
                                        username=mongo_user,
                                        password=mongo_password) 
            
        self.latency_db = self.mongo_client.thumbnail_data
        self.alternatives_db = self.mongo_client.thumbnail_alternatives
        self.cache = redis.Redis(host=redis_host, port=6379, password=redis_password)
        self.logger = logger


    def delete_zip(self, zip):
        try:
            self.logger.info("Deleting {} from S3.".format(zip))
            self.s3_client.delete_object(Bucket=bucket_name, Key=zip)
        except Exception as e:
            self.logger.info("Failed to delete zip from S3. Zip will be enqueued for deletion later. Error: {}".format(e))
            self.cache.sadd("zips_to_delete", zip)

    
    def upload_json(self, prefix, json_name):
        try:
            s3_json_name = "{}/{}".format(prefix, json_name)
            
            self.logger.info("Uploading to S3: {}".format(s3_json_name))
            self.s3_client.upload_file("{}".format(json_name), bucket_name, s3_json_name)   

            if os.path.isfile(json_name):
                os.remove(json_name)
        except Exception as e:
            self.logger.info("Error: {}. Json will be stored in {}".format(e, json_name))
            

    def insert_in_mongo(self, value):
        date = datetime.fromtimestamp(value["date"])

        if self.latency_db["latency_{}_{:02d}".format(date.year, date.month)].count_documents({"game_id": value["game_id"], "user_id": value["user_id"], "stream_id": value["stream_id"], "date": value["date"]}, limit=1) == 0:
            self.latency_db["latency_{}_{:02d}".format(date.year, date.month)].insert_one(value)
            if "_id" in value:
                value.pop("_id")
            
            self.cache.sadd("new_latency", json.dumps(value))
                
            return True
        
        return False
    

    # Tokens are still one big collection from which the data is moved
    def insert_token_in_mongo(self, value, always_insert=False):
        if self.mongo_client.tokens.latency.count_documents({"game_id": value["game_id"], "token": value["token"], "idx": value["idx"]}, limit=1) == 0:
            self.mongo_client.tokens.latency.insert_one(value)
            return True
        
        return False


    def get_to_process(self):   
        return [json.loads(x.decode("utf-8")) for x in self.cache.spop("to_postprocess", count=1)]
        
    
    def store_information(self, to_save, always_insert=False):
        if "user_id" in to_save:
            return self.insert_in_mongo(to_save)
        
        if "token" in to_save:
            return self.insert_token_in_mongo(to_save, always_insert)
        

    def store_alternative_values(self, value):        
        if "user_id" in value:
            date = datetime.fromtimestamp(value["date"])

            if self.alternatives_db["alternatives_{}_{:02d}".format(date.year, date.month)].count_documents({"game_id": value["game_id"], "user_id": value["user_id"], "stream_id": value["stream_id"], "date": value["date"]}, limit=1) == 0:
                self.alternatives_db["alternatives_{}_{:02d}".format(date.year, date.month)].insert_one(value)
                return True
        
            return False
        
        if "token" in value:
            if self.mongo_client.tokens.alternative_values.count_documents({"game_id": value["game_id"], "token": value["token"], "idx": value["idx"]}, limit=1) == 0:
                self.mongo_client.tokens.alternative_values.insert_one(value)
                return True
        
            return False

    
    def store_metadata(self, metadata_line):
        self.mongo_client.metadata.batches.insert_one(metadata_line)

    
    def update_metadata(self, batch_name, new_useful, new_token):
        old_metadata = self.mongo_client.metadata.batches.find_one({"batch_name": batch_name})
        
        if old_metadata:
            new_useful = {k: old_metadata["images_stats"]["useful"].get(k, 0) + new_useful.get(k, 0) for k in set(old_metadata["images_stats"]["useful"]) | set(new_useful)}
            if "tokens" in old_metadata:
                new_token = {k: old_metadata["tokens"]["useful"].get(k, 0) + new_token.get(k, 0) for k in set(old_metadata["tokens"]["useful"]) | set(new_token)}    
                return self.mongo_client.metadata.batches.update_one({"batch_name": batch_name}, {"$set": {"images_stats.useful": new_useful, "tokens.useful": new_token}})
            else:
                return self.mongo_client.metadata.batches.update_one({"batch_name": batch_name}, {"$set": {"images_stats.useful": new_useful}})
            
        return False

    def store_jsons(self, results_jsons):
        # Upload the results to S3 for long-term storage
        for results_json_name in results_jsons:
            self.upload_json("results", results_json_name)


    def clean_up(self, metadata):
        for zip in metadata.get("contents", []):
            self.delete_zip(zip)


    def store_to_confirm(self, batch_name):
        self.cache.sadd("to_confirm", batch_name)

    
    def insert_user_presence(self, users_presence, data_type):
        base_db = self.mongo_client.thumbnails
                
        for user_id, data in users_presence.items():
            old_data = base_db["{}_presence".format(data_type)].find_one({"user_id": user_id}, projection={"_id": False})

            if old_data:
                base_db["{}_presence".format(data_type)].find_one_and_replace({"user_id": user_id}, {"user_id": user_id, "presence": list(set([*old_data["presence"], *data]))}, upsert=True)
            else:
                # Insert directly
                base_db["{}_presence".format(data_type)].insert_one({"user_id": user_id, "presence": list(data)})


