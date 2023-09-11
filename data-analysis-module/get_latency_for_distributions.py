from pymongo import MongoClient
from config import mongo_host, mongo_port, mongo_user, mongo_password, min_cluster_coverage
from tqdm import tqdm
from utils.logger import get_logger


class DistributionLatency:
    def __init__(self):
        self.logger = get_logger("distributions")
        self.mongo_client = MongoClient("mongodb://{}:{}/".format(mongo_host, mongo_port), username=mongo_user, password=mongo_password)


    def get_all_latency(self):
        self.logger.info("Starting process...")

        self.logger.info("Cleaning old data")
        self.mongo_client.distribution.latency.drop()

        self.logger.info("Inserting data from users with changes")
        for user in tqdm(self.mongo_client.processed.changes_summary.find(projection={"_id": False})):
            for cluster in user["changes"]["found_cluster"]:
                if cluster["cluster"]["coverage"] > min_cluster_coverage:
                    for dates_subsequence in cluster["subsequence"]["dates"]:
                        subsequence_latency = [l for l in self.mongo_client.processed.latency.find({"game_id": user["game_id"], "user_id": user["user_id"], "date": {"$gte": dates_subsequence[0], "$lte": dates_subsequence[1]}}, projection={"_id": False})]
                        
                        if subsequence_latency:
                            self.mongo_client.distribution.latency.insert_many(subsequence_latency)
        
        self.logger.info("Inserting data from users without changes")
        for user in tqdm(self.mongo_client.processed.users_without_changes.find(projection={"_id": False})):
            to_insert = [l for l in self.mongo_client.processed.latency.find(user, projection={"_id": False})]
            
            if to_insert:
                self.mongo_client.distribution.latency.insert_many(to_insert)

        self.logger.info("Indexing the data")
        self.mongo_client.distribution.latency.create_index("game_id")
        self.mongo_client.distribution.latency.create_index("user_id")
        self.mongo_client.distribution.latency.create_index("date")

        self.logger.info("Finishing process...")


if __name__ == "__main__":
    distributions = DistributionLatency()
    distributions.get_all_latency()