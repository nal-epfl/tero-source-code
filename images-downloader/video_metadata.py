import redis
import json
import pytz
import re

from tqdm import tqdm
from bson import ObjectId
from dateutil import parser
from datetime import datetime, timedelta
from pymongo import MongoClient
from utils.twitch_video_utils import get_channel_videos, GQLError, InvalidUserException
from config import mongo_host, mongo_user, mongo_password, redis_host, redis_password
from logger import get_logger


localtz = pytz.timezone('Europe/Zurich')

def get_date_diff(date1, date2):
    diff = date2 - date1 if date1 < date2 else date1 - date2
    return diff


class VideoMetadataGatherer:
    def __init__(self):
        self.cache = redis.Redis(host=redis_host, port=6379, password=redis_password)
        self.mongo_client = MongoClient("mongodb://{}:27017/".format(mongo_host),
                            username=mongo_user,
                            password=mongo_password)
        self.logger = get_logger("video_metadata_gather")
        

    def get_video_data(self, user_login, stream_starting_time):
        default_time = "2020-01-01T00:00:00Z"        
            
        try:
            video_info = get_channel_videos(user_login)
        except (GQLError, InvalidUserException):
            return None

        if not video_info:
            return None

        stream_time = datetime.fromtimestamp(stream_starting_time)
        stream_time = localtz.localize(stream_time)

        should_keep = True

        while should_keep:
            for video in video_info["edges"]:
                video_time = video.get("node", {}).get("publishedAt", default_time)

                video_start = parser.parse(video_time)
                time_diff = get_date_diff(video_start, stream_time)

                if time_diff < timedelta(minutes=1):
                    return video

            should_keep = False
            if video_info["pageInfo"]["hasNextPage"] and video_info["edges"]:
                should_keep = True
                try:
                    video_info = get_channel_videos(user_login, after=video_info["edges"][-1]["cursor"])
                except (GQLError, InvalidUserException):
                    return None

        return None


    def extract_user_login(self, stream_data):
        if "thumbnail_url" in stream_data:
            m = re.search(r"live_user_(?P<user_login>\w+)-{width}x{height}.jpg", stream_data["thumbnail_url"])
            if m:
                return m.group("user_login")

        return None


    
    def get_metadata(self, stream_data):
        if "user_login" not in stream_data:
            user_login = self.extract_user_login(stream_data)
            if user_login:
                stream_data["user_login"] = user_login
            else:
                return False
            
        if "starting_time" not in stream_data:
            stored_stream_data = self.mongo_client.prod.current_streams.find_one({"stream_id": stream_data["id"]})
            if not stored_stream_data:
                return False
            
            stream_data["starting_time"] = stored_stream_data["starting_time"]

        twitch_data = self.mongo_client.videos.twitch_data.find_one({"stream_id": stream_data["id"]}, projection={"_id": False})
        if not twitch_data:
            video_data = self.get_video_data(stream_data["user_login"], stream_data["starting_time"])
                        
            if video_data:
                twitch_data = {"stream_id": stream_data["id"], "data": video_data if video_data else {}}
                self.mongo_client.videos.twitch_data.insert_one(twitch_data)
                return True

        return False    
            

    def run(self):
        self.logger.info("Starting process...")

        # Get new sampled streams and store them for later
        new_streams = [json.loads(x) for x in self.cache.spop("new-sampled-streams", count=self.cache.scard("new-sampled-streams"))]
                
        for n in new_streams:
            self.mongo_client.videos.new_streams.insert_one(n)
        
        self.logger.info("Found {} new streams".format(len(new_streams)))

        ranges_to_test = [24*7, 24*3, 48, 24, 12]
        
        already_tested = set()

        for time_range in ranges_to_test:
            self.logger.info("Trying {}".format(time_range))

            to_test = []
            # Go through all streams that started more than X hours ago
            for older in self.mongo_client.videos.new_streams.find({"starting_time": {"$lte": (datetime.now() - timedelta(hours=time_range)).timestamp()}}):
                # If you got to the last time range, this stream is absolutely done.
                if older["id"] in already_tested:
                    continue
                
                to_test.append(older)
            
            self.logger.info("\t Found {} to test".format(len(to_test)))
             
            for idx, older in enumerate(to_test):
                if idx%100 == 0:
                    self.logger.info("\t\t Tested {}/{}".format(idx, len(to_test)))

                has_metadata = self.get_metadata(older) 
                already_tested.add(older["id"])

                if has_metadata or time_range == ranges_to_test[0]:
                    _id = older.pop("_id")
                    self.mongo_client.videos.new_streams.delete_one({"_id": ObjectId(_id)})

        self.logger.info("Finished process...")


    def on_demand_search(self):
        self.logger.info("Running on-demand search...")

        on_demand_streams = [x.decode("utf8") for x in self.cache.spop("on-demand-streams", count=self.cache.scard("on-demand-streams"))]
        self.logger.info("Found to search: {}".format(len(on_demand_streams)))

        for on_demand in on_demand_streams:
            stream_data = self.mongo_client.videos.new_streams.find_one({"id": on_demand})
            if stream_data:
                self.get_metadata(stream_data)
            else:
                print("{}".format(on_demand))
                

        self.logger.info("Finished on-demand...")




if __name__ == "__main__":
    gatherer = VideoMetadataGatherer()
    gatherer.run()
    # gatherer.on_demand_search()