import redis
from pymongo import MongoClient, DESCENDING

from bson import ObjectId
from logger import get_logger
from config import mongo_host, mongo_password, mongo_user, redis_host, redis_port, redis_password

from utils.base_utils import parse_image_name


class CleanMissedFrames:
    def __init__(self):
        self.mongo_client = MongoClient("mongodb://{}:27017/".format(mongo_host),
                            username=mongo_user,
                            password=mongo_password)

        self.cache = redis.Redis(host=redis_host, port=redis_port, db=0, password=redis_password)
        self.logger = get_logger("clean-missed-frames")


    def run(self):
        self.logger.info("Starting process...")

        # Get all images that you are still expecting
        expected_per_stream = {}
        for batch in self.mongo_client.footage.expected_batches.find():
            if batch["stream_id"] not in expected_per_stream:
                expected_per_stream[batch["stream_id"]] = set()
            
            expected_per_stream[batch["stream_id"]].update(batch["expected_dates"])

        to_delete = []
        for token in self.mongo_client.footage.frames_downloaded.find():
            image_info = parse_image_name(token["frame_name"])

            if image_info["stream_id"] not in expected_per_stream:
                to_delete.append(token)
                continue

            if image_info["date"] not in expected_per_stream[image_info["stream_id"]]:
                to_delete.append(token)

        self.logger.info("Found {} frames to delete.".format(len(to_delete)))

        for td in to_delete:
            self.mongo_client.footage.frames_downloaded.delete_one({"_id": ObjectId(td["_id"])}) 

        self.logger.info("Finishing process...")


if __name__ == "__main__":
    explorer = CleanMissedFrames()
    explorer.run()
    