import json
from pymongo import MongoClient
from config import mongo_host, mongo_user, mongo_password


class OfflineStorage:
    def __init__(self):
        self.new_users = open("results/new_users.json", "w+")
        self.raw_users = open("results/raw_users.json", "w+")
        self.not_parsed = open("results/not_parsed-remote.json", "w+")
        self.conflicts = open("results/conflicts.json", "w+")
        self.mongo_client = MongoClient("mongodb://{}:27017/".format(mongo_host), username=mongo_user, password=mongo_password)

        self.useful_stored = 0


    def insert_in_cache(self, first_found, nominatim_result, parsed_location):
        self.mongo_client.location_data.cache.insert_one({"query": first_found.lower(), "geocoded": nominatim_result, "data": parsed_location})


    def insert_not_parsed(self, user_to_store):
        self.not_parsed.write(json.dumps(user_to_store) +"\n")


    def store_parsed(self, user_to_store, raw_user, twitch_id):
        if "_id" in user_to_store["location"]:
            user_to_store["location"].pop("_id")

        self.new_users.write(json.dumps(user_to_store) +"\n")
        self.raw_users.write(json.dumps(raw_user) +"\n")
        self.useful_stored += 1 
        
        
    def store_conflict(self, user):
        self.conflicts.write(json.dumps(user) +"\n")

    
    def insert_new_user(self, to_insert, logger):
        self.new_users.write(json.dumps(to_insert) +"\n")


    def delete_user(self, user):
        pass
    

    def insert_raw_user(self, raw_user):
        self.raw_users.write(json.dumps(raw_user) +"\n")