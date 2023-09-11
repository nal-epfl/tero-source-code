import json
import redis
import shutil
import boto3

from pymongo import MongoClient
from config import redis_host, redis_port, redis_password, bucket_name, mongo_host, mongo_port, mongo_user, mongo_password, long_term_storage, stream_ends_storage, rw_access_key, rw_secret_key, s3_url


class OnlineController:
    def __init__(self, logger):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=rw_access_key,
            aws_secret_access_key=rw_secret_key,
            endpoint_url=s3_url
        )
        self.mongo_client = MongoClient("mongodb://{}:{}/".format(mongo_host, mongo_port),
                                        username=mongo_user,
                                        password=mongo_password) 
        
        self.cache = redis.Redis(host=redis_host, port=redis_port, password=redis_password)
        self.logger = logger


    def delete_zip(self, zip):
        try:
            self.logger.info("Deleting {} from S3.".format(zip))
            self.s3_client.delete_object(Bucket=bucket_name, Key=zip)
        except Exception as e:
            self.logger.info("Failed to delete zip from S3. Zip will be enqueued for deletion later. Error: {}".format(e))
            self.cache.sadd("zips_to_delete", zip)

    
    def upload_json(self, prefix, json_name):
        stored_json = "{}/{}".format(long_term_storage, json_name)

        try:
            s3_json_name = "{}/{}".format(prefix, json_name)
            
            self.logger.info("Uploading to S3: {}".format(s3_json_name))
            self.s3_client.upload_file("{}".format(json_name), bucket_name, s3_json_name)
            
            shutil.move(json_name, stored_json)   
        except Exception as e:
            self.logger.info("Error: {}. Json will be stored in {}".format(e, stored_json))
            

    def insert_in_mongo(self, value):
        if self.mongo_client.data.latency.count_documents({"game_id": value["game_id"], "user_id": value["user_id"], "stream_id": value["stream_id"], "date": value["date"]}, limit=1) == 0:
            self.cache.sadd("new_latency", json.dumps(value))
            self.cache.sadd("logs_latency", json.dumps(value))

            self.mongo_client.data.latency.insert_one(value)
            return True
        
        return False


    def get_to_process(self):
        return [json.loads(x.decode("utf-8")) for x in self.cache.spop("to_postprocess", count=1)]

    
    def store_information(self, to_save):
        return self.insert_in_mongo(to_save)


    def store_alternative_values(self, value):
        if self.mongo_client.data.alternative_values.count_documents({"game_id": value["game_id"], "user_id": value["user_id"], "stream_id": value["stream_id"], "date": value["date"]}, limit=1) == 0:
            self.mongo_client.data.alternative_values.insert_one(value)
            return True
        
        return False

    
    def store_metadata(self, metadata_line):
        self.mongo_client.results.metadata.insert_one(metadata_line)

    
    def update_metadata(self, batch_name, new_useful):
        old_metadata = self.mongo_client.results.metadata.find_one({"batch_name": batch_name})
        
        if old_metadata:
            new_useful = {k: old_metadata["images_stats"]["useful"].get(k, 0) + new_useful.get(k, 0) for k in set(old_metadata["images_stats"]["useful"]) | set(new_useful)}
                    
            return self.mongo_client.results.metadata.update_one({"batch_name": batch_name}, {"$set": {"images_stats.useful": new_useful}})

        return False

    def store_jsons(self, results_jsons):
        # Upload the results to S3 for long-term storage
        for results_json_name in results_jsons:
            self.upload_json("results", results_json_name)


    def clean_up(self, metadata):
        for zip in metadata.get("contents", []):
            self.delete_zip(zip)


    def store_stream_ends(self, stream_ends, batch_name):
        with open("{}/{}-stream_ends.json".format(stream_ends_storage, batch_name), "w+") as f:
            for stream, end in stream_ends.items():
                f.write(json.dumps({"stream_id": stream, "end": end}) + "\n")
    

    def store_to_confirm(self, batch_name):
        self.cache.sadd("to_confirm", batch_name)