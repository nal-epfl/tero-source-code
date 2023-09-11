import multiprocessing

from utils.utils import get_stored_locations, get_users_by_region

from pymongo import MongoClient
from functools import partial
from utils.logger import get_logger
from db.mongo_controller import MongoController

from config import mongo_host, mongo_password, mongo_port, mongo_user

number_cores = 15


class CountryDatasetPartitioner:
    def __init__(self, game, users_by_region):
        self.game_id = game
        self.mongo_client = MongoClient("mongodb://{}:{}/".format(mongo_host, mongo_port), username=mongo_user, password=mongo_password)
        self.users_by_region = users_by_region
        self.db = self.mongo_client.partitioned


    def run(self, country):
        region_index_map = {}
        region_idx = 0

        for region, users in self.users_by_region[country].items():
            region_index_map[region] = region_idx
            
            for user in users:
                to_insert = []
                for l in self.mongo_client.processed.latency.find({"game_id": self.game_id, "user_id": user}, projection={"_id": False}):
                    to_insert.append(l)

                    if len(to_insert) > 10000:
                        self.db["latency-{}-{}-{}".format(self.game_id, country, region_idx)].insert_many(to_insert)
                        to_insert = []    

                if to_insert:
                    self.db["latency-{}-{}-{}".format(self.game_id, country, region_idx)].insert_many(to_insert)
                
                to_insert = []

                for l in self.mongo_client.processed.spikes.find({"game_id": self.game_id, "user_id": user}, projection={"_id": False}):
                    to_insert.append(l)

                    if len(to_insert) > 10000:
                        self.db["spikes-{}-{}-{}".format(self.game_id, country, region_idx)].insert_many(to_insert)
                        to_insert = []    

                if to_insert:
                    self.db["spikes-{}-{}-{}".format(self.game_id, country, region_idx)].insert_many(to_insert)
            

            self.db["latency-{}-{}-{}".format(self.game_id, country, region_idx)].create_index("user_id")
            self.db["latency-{}-{}-{}".format(self.game_id, country, region_idx)].create_index("game_id")
            self.db["latency-{}-{}-{}".format(self.game_id, country, region_idx)].create_index("date")
            self.db["spikes-{}-{}-{}".format(self.game_id, country, region_idx)].create_index("user_id")
            self.db["spikes-{}-{}-{}".format(self.game_id, country, region_idx)].create_index("game_id")
            self.db["spikes-{}-{}-{}".format(self.game_id, country, region_idx)].create_index("date")

            region_idx += 1

        self.mongo_client.shared_anomaly.region_map.insert_one({"game_id": self.game_id, "country": country, "region_map": region_index_map})



def process_game(users_by_region, game_data):
    detection = CountryDatasetPartitioner(game_data["game_id"], users_by_region)
    return detection.run(game_data["country"])



class DatasetPartitioner:
    def __init__(self):
        self.db_controller = MongoController()
        self.spikes_by_region = {}
        self.logger = get_logger("dataset_partition")
        self.users_locations = get_stored_locations(self.db_controller.mongo_client)
   

    def run(self):
        self.logger.info('Starting dataset partitioner')

        users_by_region = get_users_by_region(self.users_locations)

        game_data = []

        for game in self.db_controller.get_all_games():
            for country in users_by_region.keys():
                game_data.append({"game_id": game, "country": country})

        self.logger.info('To process: {}'.format(len(game_data)))

        pool = multiprocessing.Pool(number_cores)
        tmp = partial(process_game, users_by_region)
        
        results = pool.imap(func=tmp, iterable=game_data)

        for r in results:
            pass
            
        self.logger.info('Finished dataset partitioner')


if __name__ == "__main__":
    processor = DatasetPartitioner()
    processor.run()