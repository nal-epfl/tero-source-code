from tqdm import tqdm
from pymongo import MongoClient
from datetime import datetime, timedelta
from config import  mongo_host, mongo_password, mongo_user

from logger import get_logger


random_samples = 10
finished_threshold = 0.7
max_chunks = 20


class FinishedBatches:
    def __init__(self):
        self.mongo_client = MongoClient("mongodb://{}:27017/".format(mongo_host),
                                        username=mongo_user,
                                        password=mongo_password)
        self.chunks_counter = 0
        self.logger = get_logger("get-finished-batches")       
        self.latency_db = self.mongo_client.footage_data

    
    def fetch_batch_latency(self, expected_list, base_data):
        finished = []
        
        for expected in expected_list:
            dt = datetime.fromtimestamp(int(expected))

            latency = self.latency_db["latency_{}_{:02d}".format(dt.year, dt.month)].find_one({**base_data, "date": expected}, projection={"_id": False})
            if latency:
                finished.append(latency)

        return finished

    def process_ongoing_batches(self):
        self.logger.info("Starting process...")
        not_done = 0

        # Each batch is one QoE band
        for ongoing in tqdm(self.mongo_client.footage.expected_batches.find()):
            # It is better to treat each batch separately: if 80% of the points are done, you can consider the batch ready.
            finished_latency = self.fetch_batch_latency(ongoing["expected_dates"], {"game_id": ongoing["game_id"], "user_id": ongoing["user_id"], "stream_id": ongoing["stream_id"]})

            # There are enough points done or the batch is old enough (more than a month old)
            if len(finished_latency) < len(ongoing["expected_dates"])*finished_threshold and datetime.fromtimestamp(max([int(o) for o in ongoing["expected_dates"]])) > datetime.now() - timedelta(days=31):
                # Still not enough points are done.               
                not_done += 1
                continue
    
            _id = ongoing.pop("_id")
            self.mongo_client.footage.finished_batches.insert_one(ongoing)
            self.mongo_client.footage.expected_batches.delete_one({"_id": _id})
            self.chunks_counter += 1
    
        self.logger.info("Finishing process. Found {} finished batches. Found {} ongoing chunks.".format(self.chunks_counter, not_done))


if __name__ == "__main__":
    explorer = FinishedBatches()
    explorer.process_ongoing_batches()
    