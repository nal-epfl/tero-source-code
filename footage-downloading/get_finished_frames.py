import requests
import redis
import re
import json
import random

from datetime import datetime
from tqdm import tqdm
from bson import ObjectId
from pymongo import MongoClient, DESCENDING

from config import mongo_host, mongo_password, mongo_user, redis_host, redis_port, redis_password
from logger import get_logger

from utils.base_utils import parse_image_name


class FetchResults:
    def __init__(self):
        self.mongo_client = MongoClient("mongodb://{}:27017/".format(mongo_host),
                            username=mongo_user,
                            password=mongo_password)

        self.cache = redis.Redis(host=redis_host, port=redis_port, db=0, password=redis_password)
        self.logger = get_logger("get-finished-frames")
        self.latency_db = self.mongo_client.footage_data
        self.alternatives_db = self.mongo_client.footage_alternatives
   

    def update_presence_maps(self, users_presence, data_type):
        base_db = self.mongo_client.footage
                
        for user_id, data in users_presence.items():
            old_data = base_db["{}_presence".format(data_type)].find_one({"user_id": user_id}, projection={"_id": False})

            if old_data:
                base_db["{}_presence".format(data_type)].find_one_and_replace({"user_id": user_id}, {"user_id": user_id, "presence": list(set([*old_data["presence"], *data]))}, upsert=True)
            else:
                # Insert directly
                base_db["{}_presence".format(data_type)].insert_one({"user_id": user_id, "presence": list(data)})


    def run(self):
        self.logger.info("Starting process...")

        tokens = [x for x in self.mongo_client.footage.frames_downloaded.find().sort("_id", DESCENDING)]
        
        self.logger.info("Found {} frames to process".format(len(tokens)))
        
        finished = 0

        users_presence = {}
        alternatives_presence = {}

        for token in tqdm(tokens):
            results = self.mongo_client.tokens.latency.find_one({"token": token["token"]})
            image_info = parse_image_name(token["frame_name"])
        
            dt = datetime.fromtimestamp(int(image_info["date"]))
        
            if not results:
                # This token is for a frame that was already processed, so it should be deleted
                
                if self.latency_db["latency_{}_{:02d}".format(dt.year, dt.month)].count_documents({"game_id": image_info["game_id"], "user_id": image_info["user_id"], "stream_id": image_info["stream_id"], "date": image_info["date"]}, limit=1) != 0:
                    # self.logger.info("Found already processed: {}".format(image_info))
                    self.mongo_client.footage.frames_downloaded.delete_one({"_id": ObjectId(token["_id"])})
                
                # This token is for a frame that was found to be impossible to process
                s3_image_name = "raw/{}_{}_0.png".format(token["frame_name"].split("_")[0], token["token"])
                if self.cache.sismember("incompatible_images", s3_image_name):
                    self.cache.srem("incompatible_images", s3_image_name)
                    self.mongo_client.footage.frames_downloaded.delete_one({"_id": ObjectId(token["_id"])})

                continue
            
            # Remove the three metadata fields
            results.pop("game_id")
            results.pop("token")
            results.pop("idx") 

            result_id = results.pop("_id")                 
            latency_results = {**image_info, **results, "token": token["token"]}

            self.latency_db["latency_{}_{:02d}".format(dt.year, dt.month)].insert_one(latency_results)
            self.cache.sadd("new-footage-sample", json.dumps({"user_id": image_info["user_id"], "stream_id": image_info["stream_id"]}))

            date = datetime.fromtimestamp(int(latency_results["date"]))
            month = "latency_{}_{:02d}".format(date.year, date.month)

            if latency_results["user_id"] not in users_presence:
                users_presence[latency_results["user_id"]] = set()

            users_presence[latency_results["user_id"]].add(month)

            self.mongo_client.tokens.latency.delete_one({"_id": ObjectId(result_id)})
            
            alternatives = self.mongo_client.tokens.alternative_values.find_one({"token": token["token"]})

            if alternatives:
                alternatives_id = alternatives.pop("_id")

                alternatives_to_insert = {**image_info, "alternatives": alternatives["values"]}
                
                if self.alternatives_db["alternatives_{}_{:02d}".format(dt.year, dt.month)].count_documents({"game_id": image_info["game_id"], "user_id": image_info["user_id"], "stream_id": image_info["stream_id"], "date": image_info["date"]}, limit=1) == 0:
                    self.alternatives_db["alternatives_{}_{:02d}".format(dt.year, dt.month)].insert_one(alternatives_to_insert)

                    date = datetime.fromtimestamp(int(alternatives_to_insert["date"]))
                    month = "alternatives_{}_{:02d}".format(date.year, date.month)

                    if latency_results["user_id"] not in alternatives_presence:
                        alternatives_presence[latency_results["user_id"]] = set()

                    alternatives_presence[latency_results["user_id"]].add(month)

                self.mongo_client.tokens.alternative_values.delete_one({"_id": ObjectId(alternatives_id)})

            self.mongo_client.footage.frames_downloaded.delete_one({"_id": ObjectId(token["_id"])})        
            finished += 1

        self.logger.info("Updating the presence maps.")
        self.update_presence_maps(users_presence, data_type="latency")
        self.update_presence_maps(alternatives_presence, data_type="alternatives")

        self.logger.info("Finished processing. Total finished: {}".format(finished))



if __name__ == "__main__":
    fetch_results = FetchResults()
    fetch_results.run()