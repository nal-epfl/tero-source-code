import json
import redis

from tqdm import tqdm
from pymongo import MongoClient
from config import redis_host, redis_port, redis_password, mongo_host, mongo_port, mongo_user, mongo_password
from nlp_utils import compare_nlp_locations
from logger import get_logger


class CompareWithTags:
    def __init__(self):
        self.cache = redis.Redis(host=redis_host, port=redis_port, password=redis_password)
        self.mongo_client = MongoClient("mongodb://{}:{}/".format(mongo_host, mongo_port), username=mongo_user, password=mongo_password)  
        self.logger = get_logger("compare_with_tags")


    def run(self):
        self.logger.info("Starting process...")

        to_insert = []
        users_to_delete = []
        
        to_process = self.cache.hlen("locations_lost")

        for user_id, loc in self.cache.hgetall("locations_lost").items():
            locations = json.loads(loc.decode("utf8"))
            user_id = user_id.decode("utf8")

            tags_available = self.mongo_client.location.tags.find_one({"user_id": user_id}, projection={"_id": False})
            if tags_available:
                tag_nlp_agreement = []
                
                for l in locations:
                    countries_from_nlp = []
                    locations = []

                    if isinstance(l["location"]["location"], list):
                        countries_from_nlp.extend([x.get("country_code", "").lower() for x in l["location"]["location"]])
                        locations.extend([x for x in l["location"]["location"]])
                    else:
                        countries_from_nlp.append(l["location"]["location"].get("country_code", "").lower())
                        locations.append(l["location"]["location"])

                    for idx, country_from_nlp in enumerate(countries_from_nlp):
                        if country_from_nlp in [x.lower() for x in tags_available["countries"].keys()]:    
                            tag_nlp_agreement.append(locations[idx])

                if tag_nlp_agreement:
                    if len(tag_nlp_agreement) > 1:
                        smallest_loc = tag_nlp_agreement[0]
                        for idx in range(1, len(tag_nlp_agreement)):
                            comparison = compare_nlp_locations(smallest_loc, tag_nlp_agreement[idx])
                            if comparison == 2:
                                smallest_loc = tag_nlp_agreement[idx]
                            elif comparison < 0:
                                break
                    else:
                        to_insert.append({"twitch_id": user_id, "location": tag_nlp_agreement[0]})

            users_to_delete.append({"twitch_id": user_id, "locations": locations})
            if len(users_to_delete) % 500  == 0:
                self.logger.info("Processed: {}/{}".format(len(users_to_delete), to_process))

        self.logger.info("Inserting results. Recovered: {}".format(len(to_insert)))
        for ti in to_insert:
            self.cache.sadd("to_locate", json.dumps(ti))
            self.cache.sadd("tags_recovered", ti["twitch_id"])
              
        self.logger.info("Deleting users processed...")
        for user in users_to_delete:
            self.cache.sadd("lost_post_tags", json.dumps(user))
            self.cache.hdel("locations_lost", user["twitch_id"])
       
        self.logger.info("Finishing process...")


if __name__ == "__main__":
    compare = CompareWithTags()
    compare.run()