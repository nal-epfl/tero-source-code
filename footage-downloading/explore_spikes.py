import redis
import json

from bson import ObjectId
from pymongo import MongoClient
from config import mongo_host, mongo_user, mongo_password, redis_host, redis_password

from tqdm import tqdm
from logger import get_logger


class ObjectIdEncoder(json.JSONEncoder):
    def default(self, o):
        return str(o)


qoe_band = 15
max_retries = 3


class SpikeDecisionProcess:
    def __init__(self):
        self.mongo_client = MongoClient("mongodb://{}:27017/".format(mongo_host),
                            username=mongo_user,
                            password=mongo_password)
        
        self.cache = redis.Redis(host=redis_host, port=6379, password=redis_password)
        self.logger = get_logger("explore-spikes")


    def add_surrounding_chunks(self, stream_data, spike_ts):
        # This way we will always catch 3 chunks no matter the position of the spike in the containing chunk
        timestamps = [spike_ts-15, spike_ts-5, spike_ts, spike_ts+5, spike_ts+15]

        for ts in timestamps:   
            # Set the current     
            self.cache.sadd("chunks-to-download", json.dumps({"game_id": stream_data["game_id"], "user_id": stream_data["user_id"], "stream_id": stream_data["stream_id"], "ts_sample": ts, 
                                                                "min": -1, "max": -1, "start": -1, "end": -1}))

        return timestamps


    def get_new_streams(self):
        streams_to_explore = [json.loads(x.decode("utf8")) for x in self.cache.spop("spikes-to-explore", count=self.cache.scard("spikes-to-explore"))]
        # streams_to_explore = [json.loads(x.decode("utf8")) for x in self.cache.smembers("spikes-to-explore")]

        self.logger.info("Found {} new streams".format(len(streams_to_explore)))

        # For each stream, check if there are ongoing spikes
        # Each line is one stream with all its associated spikes
        for stream_data in streams_to_explore:
            to_analyze = {"game_id": stream_data["game_id"], "user_id": stream_data["user_id"], "stream_id": stream_data["stream_id"], "expected": []}
            # First, surround each spike (3 chunks per spike)
            for spike in stream_data["spikes"]:
                to_analyze["expected"].extend(self.add_surrounding_chunks(stream_data, spike))

            self.mongo_client.footage.ongoing_spikes.insert_one(to_analyze)


    def check_ongoing_spikes(self):
        self.logger.info("Checking on-going spikes")
        finished = 0
        all_ongoing = 0

        # Data required: user_id, stream_id, game_id, date
        for ongoing_spike in tqdm(self.mongo_client.footage.ongoing_spikes.find()):
            all_ongoing += 1
            _id = ongoing_spike.pop("_id")

            # If the entire stream is considered "done" then the spikes are done as well
            if self.mongo_client.footage.finished_streams.count_documents({"stream_id": ongoing_spike["stream_id"]}, limit=1) > 0:
                self.mongo_client.footage.finished_spikes.insert_one(ongoing_spike)
                self.mongo_client.footage.ongoing_spikes.delete_one({"_id": ObjectId(_id)})
                finished += 1
                continue
            
            # If the stream is not done, check each ts in the finished_batches pile 
            batches_ready = set()
            for ts in ongoing_spike["expected"]:
                if self.mongo_client.footage.finished_batches.count_documents({"stream_id": ongoing_spike["stream_id"], "inspected.ts": ts}, limit=1) > 0:
                    batches_ready.add(ts)
            
            # Do I need to enforce that all the batches finish? 
            if len(ongoing_spike["expected"]) == len(batches_ready):
                self.mongo_client.footage.finished_spikes.insert_one(ongoing_spike)
                self.mongo_client.footage.ongoing_spikes.delete_one({"_id": ObjectId(_id)})           
                finished += 1
        
        self.logger.info("Found {} finished spikes (out of {})".format(finished, all_ongoing))
     
 
    def run(self):
        self.logger.info("Starting process...")

        # Get new spikes-> spike data should be stored.
        self.logger.info("Fetching new spikes...")
        self.get_new_streams()

        # Check ongoing spikes, some of them may have finished
        self.logger.info("Inspecting ongoing spikes...")
        self.check_ongoing_spikes()

        self.logger.info("Finished process...")



if __name__ == "__main__":
    spike_decision = SpikeDecisionProcess()
    spike_decision.run()