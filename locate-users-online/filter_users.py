import redis
import json
import hmac
import hashlib

from pymongo import MongoClient
from config import secret_key
from logger import get_logger
from config import redis_host, redis_password, mongo_host, mongo_user, mongo_password


class UserFilter:
    def __init__(self):
        self.logger = get_logger("filter_users")
        self.cache = redis.Redis(host=redis_host, port=6379, db=0, password=redis_password)
        self.mongo_client = MongoClient("mongodb://{}:27017/".format(mongo_host), username=mongo_user, password=mongo_password)
        
    def filter(self, old_storage, new_storage, user):
        hashed_id = hmac.new(secret_key.encode("utf-8"), user["id"].encode("utf-8"), hashlib.sha1).hexdigest()

        is_not_member = not self.cache.sismember(old_storage, "{}".format(hashed_id))

        if is_not_member:
            self.cache.sadd(old_storage, "{}".format(hashed_id))
            self.cache.sadd(new_storage, "{}".format(json.dumps({"twitch_id": user["id"], "twitch_name": user["name"]})))
            
            return is_not_member

    def check_twitch(self, user):
        return self.filter("old_twitch", "twitch", user)


    def check_twitter(self, user):
        return self.filter("old_twitter", "twitter", user)
        

    def check_tweets(self, user):
        return self.filter("old_tweets", "tweets", user)


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
                


if __name__ == '__main__':
    filter = UserFilter()
    filter.get_users()
    
