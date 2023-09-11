import redis
import json

from utils.logger import get_logger
from pymongo import MongoClient
from config import mongo_host, mongo_port, mongo_user, mongo_password, redis_host, redis_port, redis_password, min_cluster_coverage



class DistributionLatency:
    def __init__(self):
        self.logger = get_logger("distributions_online")
        self.mongo_client = MongoClient("mongodb://{}:{}/".format(mongo_host, mongo_port), username=mongo_user, password=mongo_password)
        self.cache = redis.Redis(host=redis_host, port=redis_port, password=redis_password)


    def get_new_without_changes(self):
        log_entries = [json.loads(x.decode("utf8")) for x in self.cache.spop("to_distribution_no_change", count=self.cache.scard("to_distribution_no_change"))]
                       
        per_user_game = {}

        for entry in log_entries:
            if tuple([entry["game_id"], entry["user_id"]]) not in per_user_game:
                per_user_game[tuple([entry["game_id"], entry["user_id"]])] = []
            
            per_user_game[tuple([entry["game_id"], entry["user_id"]])].append(entry["since"])

        return [{"game_id": user_game[0], "user_id": user_game[1], "since": min(since_list)} for user_game, since_list in per_user_game.items()]
        
        
    def get_new_changes(self):
        log_entries = [json.loads(x.decode("utf8")) for x in self.cache.smembers("to_distribution_new_change")]
        
        per_user_game = {}

        for entry in log_entries:
            if tuple([entry["game_id"], entry["user_id"]]) not in per_user_game:
                per_user_game[tuple([entry["game_id"], entry["user_id"]])] = []
            
            per_user_game[tuple([entry["game_id"], entry["user_id"]])].append(entry["since"])

        return [{"game_id": user_game[0], "user_id": user_game[1], "since": min(since_list)} for user_game, since_list in per_user_game.items()]                           

        
    def insert_only_new(self, to_insert):
        for ti in to_insert:
            if self.mongo_client.distribution.latency.count_documents({"user_id": ti["user_id"], "game_id": ti["game_id"], "date": ti["date"]}, limit=1) == 0: 
                self.mongo_client.distribution.latency.find_one_and_replace({"user_id": ti["user_id"], "game_id": ti["game_id"], "date": ti["date"]}, ti, upsert=True)


    def get_all_latency(self):
        self.logger.info("Starting process...")

        users_with_changes = self.get_new_changes()
        self.logger.info("Users with location changes: {}".format(len(users_with_changes)))

        for user in users_with_changes:
            summary = self.mongo_client.processed.changes_summary.find_one({"game_id": user["game_id"], "user_id": user["user_id"]}, projection={"_id": False})

            # This should never happen, but just in case
            if not summary:
                continue
            
            for cluster in summary["changes"]["found_cluster"]:
                if cluster["cluster"]["coverage"] > min_cluster_coverage:
                    for dates_subsequence in cluster["subsequence"]["dates"]:
                        if dates_subsequence[1] >= user["since"]:
                            subsequence_latency = [l for l in self.mongo_client.processed.latency.find({"game_id": user["game_id"], "user_id": user["user_id"], "date": {"$gte": max(dates_subsequence[0], user["since"]), "$lte": dates_subsequence[1]}}, projection={"_id": False})]
                            
                            self.insert_only_new(subsequence_latency)
                            self.cache.sadd("to_adapter", json.dumps({"user_id": user["user_id"], "game_id": user["game_id"], "since": max(dates_subsequence[0], user["since"]), "to": dates_subsequence[1]}))

        users_without_changes = self.get_new_without_changes()
        self.logger.info("Users without location changes: {}".format(len(users_without_changes)))        
        
        for user in users_without_changes:
            to_insert = [l for l in self.mongo_client.processed.latency.find({"user_id": user["user_id"], "game_id": user["game_id"], "date": {"$gte": user["since"]}}, projection={"_id": False})]
            
            self.insert_only_new(to_insert)
            self.cache.sadd("to_adapter", json.dumps({"user_id": user["user_id"], "game_id": user["game_id"], "since": user["since"]}))

        self.logger.info("Finishing process...")


if __name__ == "__main__":
    distributions = DistributionLatency()
    distributions.get_all_latency()