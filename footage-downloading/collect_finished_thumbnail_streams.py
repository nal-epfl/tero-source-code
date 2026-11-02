import redis
import json
from datetime import datetime, timedelta
from pymongo import MongoClient

from config import  mongo_host, mongo_password, mongo_user, redis_host, redis_password
from logger import get_logger


class FinishedStreamCollector:
    def __init__(self):
        self.logger = get_logger("finished-thumbnail-streams")
        self.cache = redis.Redis(host=redis_host, port=6379, password=redis_password)

        self.mongo_client = MongoClient("mongodb://{}:27017/".format(mongo_host),
                                        username=mongo_user,
                                        password=mongo_password)
        self.latency_db = self.mongo_client.thumbnail_data

        self.users_locations = {}
        self.get_stored_locations()


    def get_stored_locations(self):
        for x in self.mongo_client.processed.locations.find({}, projection={"_id": False}):
            self.users_locations = {**self.users_locations, **x}


    def get_current_and_update(self):        
        current_streams = {}

        for stream_id, stream_data in self.cache.hgetall("current_probes").items():
            current_streams[stream_id.decode("utf8")] = json.loads(stream_data.decode("utf8"))

        self.logger.info("Fetching current streams. Found: {}".format(len(current_streams)))

        old_streams = set([x.decode("utf8") for x in self.cache.hgetall("current_streams").keys()])

        self.logger.info("Fetching old streams. Found: {}".format(len(old_streams)))

        for stream in current_streams.values():
            # This stream has seen before and hasn't finished yet
            if stream["id"] not in old_streams:
                # First time I've seen this stream
                self.cache.hset("current_streams", stream["id"], json.dumps({**stream, "ts": datetime.now().timestamp()}))

        self.logger.info("Computing finished streams. Found: {}".format(len(old_streams - set(current_streams.keys()))))

        return old_streams - set(current_streams.keys())


    def filter_finished_streams(self, finished_streams):
        # This is a filtering process: look at every single finished stream, check if they fulfil the basic conditions, and store them for the final selection later.
        are_valid_candidates = 0
        without_location = 0
        without_twitch_data = 0
        
        if finished_streams:                        
            now = datetime.now()

            for finished in finished_streams:
                # get old stream data: if it doesn't exist something is seriously wrong
                if not self.cache.hexists("current_streams", finished):
                    continue 

                stream_data = json.loads(self.cache.hget("current_streams", finished).decode("utf8"))
                
                # Now that this stream has finished, the first step is to remove it from the list.
                self.cache.hdel("current_streams", finished)   

                # User has location information up to region
                user_location = self.users_locations.get(stream_data["user_id"], {})

                # Check if you can find video data at all: only keep as candidates streams that have video data. You can store it so there's less work for later
                twitch_data = self.mongo_client.videos.twitch_data.find_one({"stream_id": finished}, projection={"_id": False})
                if not twitch_data:
                    # If no twitch data could be found, check the timestamp. If it has been less than 7*24 hours, insert back and continue
                    # Note Dec-30: Move this threshold to wait an entire 
                    if  now - timedelta(hours=7*24) < datetime.fromtimestamp(stream_data["ts"]):
                        # self.logger.info("Found a recent finished stream without twitch data, returning to pool: {}".format(finished))
                        self.cache.hset("current_streams", finished, json.dumps(stream_data))
                        continue

                # You couldn't find video data, so this stream can not be a candidate. 
                if not twitch_data:
                    without_twitch_data += 1
                    continue

                # Look for stream data in the containing collection and the previous one (I'm assuming that no stream lasts more than 1 month)
                stream_ts_dt = datetime.fromtimestamp(stream_data["ts"])
                collections_to_check = ["latency_{}_{:02d}".format((stream_ts_dt-timedelta(days=31)).year, (stream_ts_dt-timedelta(days=31)).month), "latency_{}_{:02d}".format(stream_ts_dt.year, stream_ts_dt.month)]
                
                first_point = None
                for col in collections_to_check:
                    first_latency_point = sorted([x["date"] for x in self.latency_db[col].find({"user_id": stream_data["user_id"], "stream_id": finished})])
                    if not first_latency_point:
                        continue
                    else:
                        first_point = first_latency_point[0]

                if not first_point:
                    continue

                estimated_stream_len = now - datetime.fromtimestamp(first_point)
                if estimated_stream_len < timedelta(hours=1):
                    continue

                # Finally, this stream can be added to the candidates pool: what do you need to keep?
                self.cache.sadd("streams-to-explore", json.dumps({**stream_data, **user_location, "estimated_len_hours": estimated_stream_len.seconds, "ts": now.timestamp()}))
                are_valid_candidates += 1
        
        self.logger.info("Final report: ")
        self.logger.info("\t Valid candidates: {}".format(are_valid_candidates))
        self.logger.info("\t Without location: {}".format(without_location))
        self.logger.info("\t Without video data: {}".format(without_twitch_data))


if __name__ == "__main__":
    collector = FinishedStreamCollector()
    finished_streams = collector.get_current_and_update()

    collector.filter_finished_streams(finished_streams)
        