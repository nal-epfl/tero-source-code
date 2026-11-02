import redis

from bson import ObjectId
from pymongo import MongoClient, ReturnDocument
from config import mongo_host, mongo_user, mongo_password, redis_host, redis_password

from logger import get_logger


class FinishedGatherer:
    def __init__(self):
        self.mongo_client = MongoClient("mongodb://{}:27017/".format(mongo_host),
                            username=mongo_user,
                            password=mongo_password)
        self.cache = redis.Redis(host=redis_host, port=6379, password=redis_password)    
        self.logger = get_logger("get-finished-streams")
    

    def merge(self, new_data, previous_data=None):
        by_start_time = {}
    
        if previous_data:
            for band in previous_data["bands"]:
                if band["start"] not in by_start_time:
                    by_start_time[band["start"]] = {**band, "ts": []}
                by_start_time[band["start"]]["ts"].extend(band["ts"])

        for band in new_data:
            if band["start"] not in by_start_time:
                by_start_time[band["start"]] = {**band, "ts": []}
            by_start_time[band["start"]]["ts"].append(band["ts"])

        return list(by_start_time.values())


    def run(self):
        self.logger.info("Starting process...")
        # Get all streams with batches still ongoing
        ongoing = {}
        for x in self.mongo_client.footage.expected_batches.find():
            if x["stream_id"] not in ongoing:
                ongoing[x["stream_id"]] = 0
            ongoing[x["stream_id"]] += 1

        self.logger.info("Found {} ongoing streams".format(len(ongoing)))

        finished_batches_by_stream = {}

        for batch in self.mongo_client.footage.finished_batches.find():
            if batch["stream_id"] in ongoing:
                continue

            if batch["stream_id"] not in finished_batches_by_stream:
                finished_batches_by_stream[batch["stream_id"]] = {}
            
            if batch["game_id"] not in finished_batches_by_stream[batch["stream_id"]]:
                finished_batches_by_stream[batch["stream_id"]][batch["game_id"]] = []

            finished_batches_by_stream[batch["stream_id"]][batch["game_id"]].append(batch)

        self.logger.info("Found {} finished streams".format(len(finished_batches_by_stream)))

        # Compile the stream data and clean
        for stream_id, games in finished_batches_by_stream.items():
            for game_id, batches in games.items():
                # Fetch existing  information for this stream and game
                bands = []
                
                if self.mongo_client.footage.finished_streams.count_documents({"game_id": game_id, "stream_id": stream_id}, limit=1) != 0:
                    existing_data = self.mongo_client.footage.finished_streams.find_one({"game_id": game_id, "stream_id": stream_id})
                    bands = self.merge([x["inspected"] for x in batches], existing_data)
                else:
                    # If no previous data is there
                    bands = self.merge([x["inspected"] for x in batches])

                stream_game_data = {"game_id": game_id, "user_id": batches[0]["user_id"], "stream_id": stream_id, "bands": bands}
                
                # finished_streams.insert_one -> this is an upsert!
                self.mongo_client.footage.finished_streams.find_one_and_replace({"game_id": game_id, "user_id": batches[0]["user_id"], "stream_id": stream_id}, stream_game_data, upsert=True, return_document=ReturnDocument.AFTER)
                # Add finished explored stream here to redis, then compute variance in a different process
                                
                for batch in batches:
                    self.mongo_client.footage.finished_batches.delete_one({"_id": ObjectId(batch["_id"])})
                    

        self.logger.info("Finished process...")            


if __name__ == "__main__":
    gatherer = FinishedGatherer()
    gatherer.run()