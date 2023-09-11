import redis
import json

from datetime import datetime


class LocalController:
    def __init__(self, storage_dir, redis_conf):
        self.storage_dir = storage_dir
        self.users_storage = redis.Redis(host=redis_conf["host"], port=redis_conf["port"], password=redis_conf["password"])
        self.now = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        self.buffer = open("{}/{}.json".format(self.storage_dir, self.now), "a+")
    
    def save_streams(self, data):
        self.buffer.write(json.dumps(data) + "\n")
               
    def save_users(self, data):
        self.users_storage.sadd("new_users", json.dumps({"users": data, "timestamp": datetime.now().timestamp()}))

    def finish(self):
        self.users_storage.sadd("stream_files", "{}.json".format(self.now))
        self.users_storage.sadd("new_stream_files", "{}.json".format(self.now))