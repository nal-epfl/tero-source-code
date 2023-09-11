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

from config import redis_host, redis_password, redis_port, users_batch_size, default_to_sleep, max_queue_size, min_queue_size, secret_key, base_path
   

class ThumbnailDownloader:
    def __init__(self):
        self.offline = []
        
        self.storage = redis.Redis(host=redis_host, port=redis_port, db=0, password=redis_password)
        self.logger = get_logger("thumbnails_downloader")
        self.last_process_check = datetime.now()

    def loop(self):
        while True:            
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
                    stream = json.dumps({'id': hmac.new(secret_key.encode("utf-8"), s["stream_id"].encode("utf-8"), hashlib.sha1).hexdigest(), 'user_id': hmac.new(secret_key.encode("utf-8"), 
                                        s["user_id"].encode("utf-8"), hashlib.sha1).hexdigest(), 'game_id': s["game_id"], 'thumbnail_url': s["url"], 'twitch_id': s["user_id"]})

                    if not self.storage.hexists("current_probes", stream_id):
                        new_streams_online.add(stream_id)
                        self.storage.sadd("to_download", stream)

                    self.storage.hset("current_probes", stream_id, stream)

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
        
        now = datetime.now()

        offline_to_delete = self.storage.zrangebyscore("offline", 0, int((now - timedelta(minutes=30)).timestamp()))
        finished_to_delete = self.storage.zrangebyscore("finished", 0, int((now - timedelta(hours=6)).timestamp()))

        if offline_to_delete:
            self.logger.info("Deleting expired offline users: {}".format(len(offline_to_delete)))
            self.storage.zremrangebyscore("offline", 0, int((now - timedelta(minutes=30)).timestamp()))
            self.storage.srem("queried", *offline_to_delete)
        
        if finished_to_delete:
            self.logger.info("Deleting expired finished users: {}".format(len(finished_to_delete)))
            self.storage.zremrangebyscore("finished", 0, int((now - timedelta(hours=6)).timestamp()))

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


    def split_queue(self, index, new_queue):
        try:
            in_queue = self.storage.scard("streams_{}".format(index))
            to_move = self.storage.spop("streams_{}".format(index), count=int(in_queue/2))

            if to_move:
                self.storage.sadd("streams_{}".format(new_queue), *to_move)
                subprocess.Popen(["nohup", "{}/venv/bin/python3".format(base_path), "{}/downloader.py".format(base_path), str(new_queue)], 
                                stdout=open('/dev/null', 'w'),
                                stderr=open('/dev/null', 'a'),
                                preexec_fn=os.setpgrp)

                return True
            return False
        except Exception as e:
            return False


    def remove_queue(self, index, last_idx):
        try:
            # Empty the last process' queue first so that it stops popping new streams
            in_queue = self.storage.scard("streams_{}".format(last_idx))

            # Send the stop signal to the last queue
            self.storage.sadd("streams_{}_stop".format(last_idx))

            # Pop gets count elements OR all, as the two operations are not atomic, in_queue*2 is there to 
            # make sure we get all elements even if the process fetched something between these two lines.
            to_move = self.storage.spop("streams_{}".format(last_idx), count=int(in_queue*2))

            if index != last_idx:
                # Get the size of the "short" queue
                in_queue_short = self.storage.scard("streams_{}".format(index))

                # Now add the last process' elements to the short queue: this way you make sure that only the last 
                # process dies
                self.storage.sadd("streams_{}".format(index), *to_move)

                # Pop the right number of elements from the "short" queue
                to_distribute = self.storage.spop("streams_{}".format(index), count=int(in_queue_short))
            else:
                to_distribute = to_move

            batches = self.get_batches(to_distribute, last_idx - 1)

            # Add each batch to the remaining queues
            for idx in range(0, last_idx):
                self.storage.sadd("streams_{}".format(idx), *batches[idx])
                
        except Exception as e:
            print(e)
            return False


    def check_queues(self):
        idx = 0
        
        queues = []

        while True:
            in_queue = self.storage.scard("streams_{}".format(idx))

            if in_queue == 0:
                break
            
            queues.append(in_queue)
            idx += 1
        
        to_split = [idx for idx, queue in enumerate(queues) if queue > max_queue_size]
        to_delete = [idx for idx, queue in enumerate(queues) if queue < min_queue_size]

        if to_split:
            self.logger.info("Found queues that need spliting: {}".format(to_split))

            for idx in to_split:
                new_queue = len(queues)

                if self.split_queue(idx, new_queue):
                    queues.append(new_queue)
        elif to_delete:
            self.logger.info("Found queues that need to be deleted: {}".format(to_delete[0]))
            self.remove_queue(to_delete[0], len(queues) - 1)


if __name__ == '__main__':
    downloader = ThumbnailDownloader()
    downloader.loop()