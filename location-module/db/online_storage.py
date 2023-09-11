import json
import redis

from datetime import datetime
from pymongo import MongoClient
from config import mongo_host, mongo_port, redis_host, redis_port, redis_password, mongo_user, mongo_password


class OnlineStorage:
    def __init__(self):
        self.mongo_client = MongoClient("mongodb://{}:{}/".format(mongo_host, mongo_port), username=mongo_user, password=mongo_password)  
        self.cache = redis.Redis(host=redis_host, port=redis_port, db=0, password=redis_password)
        self.useful_stored = 0

    def insert_in_cache(self, first_found, nominatim_id, nominatim_result):
        self.mongo_client.location.cache.insert_one({"query": first_found.lower(), "data": {"id": nominatim_id, "data": nominatim_result}})


    def insert_not_parsed(self, user_to_store):
        self.mongo_client.location.not_parsed.insert_one(user_to_store)


    def store_parsed(self, user_to_store, twitch_id):
        self.cache.sadd("parsed_users", json.dumps(user_to_store))
        self.cache.sadd("to_probe", str(twitch_id))
        self.useful_stored += 1

    
    def store_conflict(self, user):
        self.mongo_client.location.conflicts.insert_one(user)

    
    def insert_new_user(self, to_insert):
        if self.mongo_client.location.users.count_documents(to_insert, limit=1) == 0:
            self.mongo_client.location.users.insert_one(to_insert)

        if self.mongo_client.user_stats.located_users.count_documents({"user_id": to_insert["user_id"]}, limit=1) == 0:
            self.mongo_client.user_stats.located_users.insert_one({"user_id": to_insert["user_id"], "first_seen": datetime.now().timestamp(), "last_seen": datetime.now().timestamp()})


    def delete_user(self, user):
        self.mongo_client.location.users.delete_one({"_id": user["_id"]})
    