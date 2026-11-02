import redis
import json
import random

from pymongo import MongoClient
from datetime import datetime, timedelta
from config import  mongo_host, mongo_password, mongo_user, redis_host, redis_password

from logger import get_logger

random_samples = 10
finished_threshold = 0.7
max_chunks = 20


class UserExplorer:
    def __init__(self):
        self.cache = redis.Redis(host=redis_host, port=6379, password=redis_password)
        self.mongo_client = MongoClient("mongodb://{}:27017/".format(mongo_host),
                                        username=mongo_user,
                                        password=mongo_password)
        self.chunks_counter = 0
        self.logger = get_logger("explore_streams")        


    
    def get_new_streams(self):
        # Get all finished streams in the chosen regions that have the desired properties
        # streams_to_study = [json.loads(x) for x in self.cache.spop("streams-to-explore", count=self.cache.scard("streams-to-explore"))]
        streams_to_study = [json.loads(x) for x in self.cache.smembers("streams-to-explore")]
        
        self.logger.info("Checking new streams, found: {}".format(len(streams_to_study)))

        for stream_data in streams_to_study:
            stream_ends = self.mongo_client.thumbnails.stream_ends.find_one({"stream_id": stream_data["id"]}, projection={"_id": False})

            if not stream_ends:
                # If the stream does not have latency, nothing to do: if the stream is too old, delete it
                if datetime.now() - datetime.fromtimestamp(stream_data["ts"]) > timedelta(days=14):
                    self.cache.srem("streams-to-explore", json.dumps(stream_data))
                
                print("No stream end data: {}".format(stream_data["id"]))
                continue
                            
            stream_start = stream_ends["start"]
            stream_end = stream_ends["end"]

            # Check if the stream has a spike
            spikes = [x["date"] for x in self.mongo_client.thumbnails.spikes.find({"stream_id": stream_data["id"]})]

            if spikes:
                # If there are at least one spike, send them to be explored.
                self.cache.sadd("spikes-to-explore", json.dumps({"game_id": stream_data["game_id"], "user_id": stream_data["user_id"], "stream_id": stream_data["id"], "spikes": spikes}))

            qoe_bands = []
            for x in self.mongo_client.thumbnails.qoe_sequences.find({"game_id": stream_data["game_id"], "user_id": stream_data["user_id"], "end": {"$gte": stream_start}}, projection={"_id": False}).sort("start"):
                # Important: Skip non-stable qoe bands, those are unstable for a reason
                if not x["stable"]:
                    continue
                    
                if stream_end < x["start"]:
                    break
                qoe_bands.append(x)
            
            # For each stable QoE band, select three points at random to sample.
            for qoe_band in qoe_bands:
                self.add_inspection_qoe({**stream_data, "stream_id": stream_data["id"]}, qoe_band)
        
            if qoe_bands:
                self.cache.srem("streams-to-explore", json.dumps(stream_data))    


    def add_inspection_qoe(self, stream_data, qoe_band, to_skip=set()):
        latency_in_band = [x for x in range(int(qoe_band["start"]), int(qoe_band["end"])) if x not in to_skip]

        if not latency_in_band:
            return False

        # This should never happen, but you never know
        if len(latency_in_band) < random_samples:
            # All should be selected
            for l in latency_in_band:
                self.cache.sadd("chunks-to-download", json.dumps({"game_id": stream_data["game_id"], "user_id": stream_data["user_id"], "stream_id": stream_data["stream_id"], "ts_sample": l, 
                                                                  "min": qoe_band["min"], "max": qoe_band["min"], "start": qoe_band["start"], "end": qoe_band["end"]}))
                self.chunks_counter += 1

        for _ in range(0, random_samples):
            selected = random.choice(latency_in_band)
            self.cache.sadd("chunks-to-download", json.dumps({"game_id": stream_data["game_id"], "user_id": stream_data["user_id"], "stream_id": stream_data["stream_id"], "ts_sample": selected, 
                                                              "min": qoe_band["min"], "max": qoe_band["min"], "start": qoe_band["start"], "end": qoe_band["end"]}))
            self.chunks_counter += 1

        return True
   
    


if __name__ == "__main__":
    explorer = UserExplorer()
    explorer.get_new_streams()
    