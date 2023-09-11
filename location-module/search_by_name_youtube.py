import redis
import json
import requests
import pycountry

from datetime import datetime
from pymongo import MongoClient
from time import sleep
from logger import get_logger
from config import redis_host, redis_port, storage, redis_password, youtube_api_key, mongo_host, mongo_port, mongo_password, mongo_user

batch_size = 100
path_to_storage = "{}/youtube".format(storage)
sleep_time = 5
daily_quota = 10000
safety_factor = 0.8


class LocationFromYoutube:
    def __init__(self):
        self.logger = get_logger("search_youtube_by_name")
        self.cache = redis.Redis(host=redis_host, port=redis_port, db=0, password=redis_password)
        self.mongo_client = MongoClient("mongodb://{}:{}/".format(mongo_host, mongo_port), username=mongo_user, password=mongo_password)
        

    def get_channel_by_username(self, user_name):
        url = "https://youtube.googleapis.com/youtube/v3/channels?part=snippet&forUsername={}" \
            "&key={}".format(user_name, youtube_api_key)

        response = requests.request("GET", url)

        if response.status_code == 200:
            return json.loads(response.text)
        else:
            self.logger.info("[{}] Searching user in Youtube failed. User: {}".format(response.status_code, user_name))


    def format_country(self, country_data):
        return {"country_code": country_data.alpha_2.lower(), "country": country_data.name}


    def parse_youtube(self, twitch_data, youtube_data):
        if "items" not in youtube_data:
            return

        for item in youtube_data['items']:
            if "snippet" in item: 
                description = item['snippet'].get('description', '').lower()

                if "twitch" in description and twitch_data['twitch_name'].lower() in description:
                    if "country" in item['snippet']:
                        try:
                            country_data = pycountry.countries.lookup(item['snippet']['country'].lower())
                            
                            return {"twitch_id": twitch_data['twitch_id'], "youtube_id": item['id'], "location": self.format_country(country_data)}
                        except LookupError:
                            continue
                    else:
                        # Even if there is no location available, return mapping
                        return {"twitch_id": twitch_data['twitch_id'], "youtube_id": item['id']}
                        
        return {}


    def store_raw(self, raw):
        self.logger.info("Inserting raw results")

        now = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        with open("{}/{}_raw.json".format(path_to_storage, now), 'w+') as f:
            json.dump({"data": raw}, f)


    def run(self):
        self.logger.info("Started processing")
        to_process = self.cache.spop("youtube", count=batch_size)
        n_to_process = len(to_process)

        self.logger.info("Users to process: {}".format(len(to_process)))    
        quota_used_list = [int(x.decode("utf8")) for x in self.cache.smembers("youtube_quota")]

        quota_used = quota_used_list[0] if quota_used_list else 0
        self.logger.info("Checking quota used: {}".format(quota_used))

        if quota_used > daily_quota*safety_factor:
            return

        to_locate = []
        users = [json.loads(d) for d in to_process] 
        raw_results = []

        names_found = 0
        names_connected = 0
        extra_quota_used = 0
        
        for idx, user in enumerate(users):
            if quota_used > daily_quota*safety_factor:
                self.logger.info("Ending prematurely, quota exceeded.")
                break

            try:
                youtube_data = self.get_channel_by_username(user["twitch_name"])
                quota_used += 1
                extra_quota_used += 1

                raw_results.append({"twitch": user, "youtube": youtube_data})

                if youtube_data:
                    names_found += 1
                    parsed_data = self.parse_youtube(user, youtube_data)

                    if parsed_data:
                        names_connected += 1
                        self.mongo_client.youtube.connections.insert_one(parsed_data)

                        if "location" in parsed_data:
                            to_locate.append(parsed_data)
                
                sleep(2)
                if idx%10 == 0:
                    self.logger.info("Processed: {}/{}".format(idx, len(users)))
            except Exception as e:
                self.logger.info("Critical failure: saving pending users and exiting.")
                self.cache.sadd("youtube", *to_process[idx:])

                self.logger.info("Inserting quota used: {}".format(quota_used))
                self.cache.sadd("youtube_quota", quota_used)

                self.store_raw(raw_results)

                return    

        self.store_raw(raw_results)

        old_quota = [int(x.decode("utf8")) for x in self.cache.spop("youtube_quota", count=1)]
        old_quota = old_quota[0] if old_quota else 0
       
        self.logger.info("Inserting quota used: {}".format(old_quota + extra_quota_used))
        self.cache.sadd("youtube_quota", old_quota + extra_quota_used)

        self.logger.info("Found {} users to locate".format(len(to_locate)))
        if to_locate:
            for tl in to_locate:
                if "_id" in tl:
                    tl.pop("_id")
                    
                self.cache.sadd("to_locate", json.dumps(tl))                       

        self.logger.info("Storing statistics")
        self.mongo_client.user_stats.youtube.insert_one({"ts": datetime.now().timestamp(), "input_users": n_to_process, "names_found": names_found, "names_connected": names_connected, "with_location": len(to_process)})

        self.logger.info("Finishing process")


if __name__ == '__main__':
    youtube_processor = LocationFromYoutube()
    youtube_processor.run()
