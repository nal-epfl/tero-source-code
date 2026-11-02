import hmac
import hashlib
import time
import redis
import json
import subprocess
import os

from time import sleep
from datetime import datetime, timedelta
from logger import get_logger
from pymongo import MongoClient
from dateutil import parser

from config import redis_host, redis_password, redis_port, users_batch_size, default_to_sleep, process_check_frequency, max_queue_size, min_queue_size, secret_key, mongo_host, mongo_user, mongo_password
   

class ThumbnailDownloader:
    def __init__(self):
        self.offline = []
        
        self.storage = redis.Redis(host=redis_host, port=redis_port, db=0, password=redis_password)
        self.mongo_client = MongoClient("mongodb://{}:27017/".format(mongo_host),
                                        username=mongo_user,
                                        password=mongo_password)
                                        
        self.logger = get_logger("thumbnails_downloader")
        self.last_process_check = datetime.now()

    def loop(self):
        self.logger.info("Starting loop...")

        while True:            
            # There's a message from one of the workers
            msgs = self.storage.spop("from_workers", count=self.storage.scard("from_workers"))

            if msgs:
                finished = []

                for message in msgs:
                    msg = json.loads(message) 

                    if 'finished' in msg:
                        self.logger.info("Received {} finished users from worker {}".format(len(msg['finished']), msg["origin"]))
                        finished.extend(msg["finished"])              
                
                self.update_storage([s[0] for s in finished], "finished")
                self.update_current_probes([s[1] for s in finished])

            online = self.storage.spop("twitch_api_online", count=self.storage.scard("twitch_api_online"))
            if online:
                self.logger.info("API process is ready. Users online: {}".format(len(online)))
                new_streams_online = set()

                for user_online in online:    
                    s = json.loads(user_online)
                    stream_id = hmac.new(secret_key.encode("utf-8"), s["stream_id"].encode("utf-8"), hashlib.sha1).hexdigest()
                    stream = {'id': hmac.new(secret_key.encode("utf-8"), s["stream_id"].encode("utf-8"), hashlib.sha1).hexdigest(), 'user_id': hmac.new(secret_key.encode("utf-8"), 
                              s["user_id"].encode("utf-8"), hashlib.sha1).hexdigest(), 'game_id': s["game_id"], 'thumbnail_url': s["url"], 'twitch_id': s["user_id"]}

                    if not self.storage.hexists("current_probes", stream_id):
                        # Add entry to current streams
                        self.mongo_client.thumbnails.current_streams.insert_one({"user_id": stream["user_id"], "stream_id": stream["id"], "starting_time": parser.parse(s["starting_time"]).timestamp(), "twitch_id": stream["twitch_id"]})
                        self.storage.sadd("new-sampled-streams", json.dumps({**stream, "user_login": s["user_login"], "starting_time": parser.parse(s["starting_time"]).timestamp()}))

                        new_streams_online.add(stream_id)
                        self.storage.sadd("to_download", json.dumps(stream))

                    self.storage.hset("current_probes", stream_id, json.dumps(stream))

                self.logger.info("New users online: {}".format(len(new_streams_online)))

            offline = self.storage.spop("twitch_api_offline", count=self.storage.scard("twitch_api_offline"))
            if offline:
                self.update_storage(offline, "offline")

            if self.storage.scard("twitch_api_to_query") < users_batch_size:
                users = self.get_more_users()
                if users:
                    self.storage.sadd("twitch_api_to_query", *users)    

            sleep(default_to_sleep)
            self.storage.sadd("users_tracked_log", json.dumps({"timestamp": datetime.now().timestamp(), "users": self.storage.hlen("current_probes")}))                


    def get_more_users(self):
        users_to_probe = []       

        for x in self.storage.sdiff("to_probe", "queried"):
            users_to_probe.append(x.decode("utf-8"))
            self.storage.sadd("queried", x.decode("utf-8"))

            if len(users_to_probe) >= users_batch_size:
                break

        if users_to_probe:
            self.logger.info("Found {} users to probe".format(len(users_to_probe)))
        
        if (self.storage.scard("to_probe") - self.storage.scard("queried")) < 0.1*self.storage.scard("to_probe"):
            now = datetime.now()

            offline_to_delete = self.storage.zrangebyscore("offline", 0, int((now - timedelta(hours=1)).timestamp()))
            finished_to_delete = self.storage.zrangebyscore("finished", 0, int((now - timedelta(hours=12)).timestamp()))

            if offline_to_delete:
                self.logger.info("Deleting expired offline users: {}".format(len(offline_to_delete)))
                self.storage.zremrangebyscore("offline", 0, int((now - timedelta(hours=1)).timestamp()))
                self.storage.srem("queried", *offline_to_delete)
            
            if finished_to_delete:
                self.logger.info("Deleting expired finished users: {}".format(len(finished_to_delete)))
                self.storage.zremrangebyscore("finished", 0, int((now - timedelta(hours=12)).timestamp()))
                
        return users_to_probe


    def update_storage(self, users, storage):
        user_map = {}
        for user in users:
            user_map[user] = int(datetime.now().timestamp())

        if user_map:
            self.storage.zadd(storage, user_map)


    def update_current_probes(self, streams):
        for stream in streams:
            self.storage.hdel("current_probes", stream)
        
        self.logger.info("Updated current streamers. Current number: {}".format(self.storage.hlen("current_probes")))


    def get_batches(self, streams, number_workers):
        batches = [[] for _ in range(number_workers)]

        next_id = 0

        for s in streams:
            batches[next_id].append(s)

            next_id += 1
            if next_id >= number_workers:
                next_id = 0

        return batches



if __name__ == '__main__':
    downloader = ThumbnailDownloader()
    downloader.loop()