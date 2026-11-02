import json
import redis

from datetime import datetime
from pymongo import MongoClient
from config import mongo_host, redis_host, redis_password, mongo_user, mongo_password


class OnlineStorage:
    def __init__(self):
        self.mongo_client = MongoClient("mongodb://{}:27017/".format(mongo_host), username=mongo_user, password=mongo_password)  
        self.cache = redis.Redis(host=redis_host, port=6379, db=0, password=redis_password)
        self.useful_stored = 0

    def insert_in_cache(self, first_found, nominatim_result, parsed_location):
        self.mongo_client.location_data.cache.insert_one({"query": first_found.lower(), "geocoded": nominatim_result, "data": parsed_location})


    def insert_not_parsed(self, user_to_store):
        self.mongo_client.location_data.not_parsed.insert_one(user_to_store)


    def store_parsed(self, user_to_store, raw_user, twitch_id):
        self.cache.sadd("parsed_users", json.dumps({"raw_user": raw_user, "parsed_user": user_to_store}))
        self.cache.sadd("to_probe", str(twitch_id))
        self.useful_stored += 1

    
    def store_conflict(self, user):
        self.mongo_client.location_data.conflicts.insert_one(user)

    
    def insert_new_user(self, to_insert, logger):
        if self.mongo_client.location_data.users.count_documents(to_insert, limit=1) == 0:
            self.mongo_client.location_data.users.insert_one(to_insert)                        

    def delete_user(self, user):
        self.mongo_client.location_data.users.delete_one({"_id": user["_id"]})
    

    def insert_raw_user(self, raw_user):
        self.mongo_client.location_data.raw_users.insert_one(raw_user) 