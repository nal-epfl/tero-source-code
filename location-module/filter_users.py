import redis
import json
import hmac
import hashlib

from pymongo import MongoClient
from config import secret_key
from logger import get_logger
from config import redis_host, redis_port, redis_password, mongo_host, mongo_port, mongo_user, mongo_password


class UserFilter:
    def __init__(self):
        self.logger = get_logger("filter_users")
        self.cache = redis.Redis(host=redis_host, port=redis_port, db=0, password=redis_password)
        self.mongo_client = MongoClient("mongodb://{}:{}/".format(mongo_host, mongo_port), username=mongo_user, password=mongo_password)
        
    def filter(self, old_storage, new_storage, user):
        hashed_id = hmac.new(secret_key.encode("utf-8"), user["id"].encode("utf-8"), hashlib.sha1).hexdigest()

        is_not_member = not self.cache.sismember(old_storage, "{}".format(hashed_id))

        if is_not_member:
            self.cache.sadd(old_storage, "{}".format(hashed_id))
            self.cache.sadd(new_storage, "{}".format(json.dumps({"twitch_id": user["id"], "twitch_name": user["name"]})))
            
            return is_not_member

    def check_twitch(self, user):
        return self.filter("old_twitch", "twitch", user)


    def check_steam(self, user):
        return self.filter("old_steam", "steam", user)


    def check_youtube(self, user):
        return self.filter("old_youtube", "youtube", user)


    def get_users(self):
        self.logger.info("Starting processing")
        users_lists = self.cache.spop("new_users", count=self.cache.scard("new_users"))

        for users_list in users_lists:
            users = json.loads(users_list.decode("utf-8")).get("users", [])
            
            for user in users:
                self.check_twitch(user)
                self.check_steam(user)
                self.check_youtube(user)

        for users_list in users_lists:
            json_data = json.loads(users_list.decode("utf-8"))
            users = json_data.get("users", [])

            for user in users:
                hashed_id = hmac.new(secret_key.encode("utf-8"), user["id"].encode("utf-8"), hashlib.sha1).hexdigest()

                if self.mongo_client.user_stats.located_users.count_documents({"user_id": hashed_id}, limit=1) != 0:
                    self.mongo_client.user_stats.located_users.update_one({"user_id": hashed_id}, {"$set": {"last_seen": json_data.get("timestamp")}})

                if self.mongo_client.user_stats.all_users.count_documents({"user_id": hashed_id}, limit=1) != 0:
                    self.mongo_client.user_stats.all_users.update_one({"user_id": hashed_id}, {"$set": {"last_seen": json_data.get("timestamp")}})
                else:
                    self.mongo_client.user_stats.all_users.insert_one({"user_id": hashed_id, "first_seen": json_data.get("timestamp"), "last_seen": json_data.get("timestamp")})





if __name__ == '__main__':
    filter = UserFilter()
    filter.get_users()
    
