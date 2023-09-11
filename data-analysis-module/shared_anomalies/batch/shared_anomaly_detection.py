import math
import multiprocessing
import more_itertools as mit

from random import shuffle
from tqdm import tqdm
from datetime import datetime, timedelta
from utils.utils import get_biggest_division, get_stored_locations, get_users_by_region

from config import grouping_window_size, reference_date
from functools import partial
from utils.logger import get_logger
from db.mongo_controller import MongoController


logger = get_logger("shared_anomaly_detection")
number_cores = 15
chunk_size = 100


class GameSharedAnomalyDetection:
    def __init__(self, users_by_region, idx):
        self.db_controller = MongoController()
        self.users_by_region = users_by_region
        self.db = self.db_controller.mongo_client.partitioned
        self.idx = idx


    def get_spikes_in_range(self, game_id, country, region_idx, start_date, end_date, users):
        spikes_in_range = []
        for user in users:
            for x in self.db["spikes-{}-{}-{}".format(game_id, country, region_idx)].find({"user_id": user, "date": {"$gte": start_date.timestamp()}}, projection={"_id": False}).sort("date"):
                if end_date.timestamp() < x["date"]:
                    break    
                spikes_in_range.append(x)
            
        return spikes_in_range


    def get_latency_in_range(self, game_id, country, region_idx, start_date, end_date, users):
        latency_in_range = []
        for user in users:
            for x in self.db["latency-{}-{}-{}".format(game_id, country, region_idx)].find({"user_id": user, "date": {"$gte": start_date.timestamp()}}, projection={"_id": False}).sort("date"):
                if end_date.timestamp() < x["date"]:
                    break   
                latency_in_range.append(x)
            
        return latency_in_range


    def run(self, data):
        country = data["country"]
        region = data["region"]
        spikes = data["spikes"]
        game_id = data["game_id"]

        window_td = (timedelta(minutes=grouping_window_size)/2)
        
        region_idx_map = self.db_controller.mongo_client.shared_anomaly.region_map.find_one({"game_id": game_id, "country": country})
        if not region_idx_map:
            return

        region_idx_map = region_idx_map["region_map"]

        print('[{}] Processing: {}, {}. Number of spikes: {}'.format(game_id, region, country, len(spikes)))
                    
        parameters = [x for x in self.db_controller.get_parameters(game_id, country, region)]

        if not parameters or parameters[0]["significance"] <= 10:
            return

        active_days_region = self.db_controller.get_active_days(game_id, self.users_by_region[country][region])
        
        for spike in tqdm(spikes):
            spike_date = spike["date"] if isinstance(spike["date"], datetime) else datetime.fromtimestamp(spike["date"]) 
            days = set([((spike_date - window_td) - reference_date).days,((spike_date + window_td) - reference_date).days])
            active_users = set()
            for day in days:
                active_users.update(active_days_region.get(day, []))

            other_spikes = self.get_spikes_in_range(game_id, country, region_idx_map[region], spike_date-window_td, spike_date+window_td, active_users)
            overlaps = self.get_latency_in_range(game_id, country, region_idx_map[region], spike_date-window_td, spike_date+window_td, active_users)
                
            N = len(set([x["user_id"] for x in overlaps]))
            D = len(set([x["user_id"] for x in other_spikes]))
            
            probability = math.comb(N, D) * parameters[0]["p_e"]**D  * (1 - parameters[0]["p_e"])**(N - D)
            under_threshold = probability < 0.01/100

            if under_threshold:
                general_info = {"N": N, "D": D, "prob": probability, "spike": spike, "game": game_id, "country": country, "region": region}
                detailed_info = {"other_anomalies": other_spikes, "all_overlaps": overlaps}

                self.db_controller.store_shared_anomaly(general_info, detailed_info)
    


def process_game(users_by_region, data):    
    for idx, d in enumerate(data["data"]):
        detection = GameSharedAnomalyDetection(users_by_region)
        detection.run(d)
        logger.info("[{}] {}/{}".format(data["idx"], idx, len(data["data"])))


class SharedAnomalyDetection:
    def __init__(self):
        self.db_controller = MongoController()
        self.spikes_by_region = {}
        self.users_locations = get_stored_locations(self.db_controller.mongo_client)


    def divide_spikes_by_region(self, spikes):        
        logger.info('Dividing spikes by region')
        for e in spikes:
            location = self.users_locations.get(e["user_id"], {})
        
            if location:
                division, _ = get_biggest_division(location)

                if not division:
                    continue

                if e["game_id"] not in self.spikes_by_region:
                    self.spikes_by_region[e["game_id"]] = {}

                if location["country_code"] not in self.spikes_by_region[e["game_id"]]:
                    self.spikes_by_region[e["game_id"]][location["country_code"]] = {}
                
                if division not in self.spikes_by_region[e["game_id"]][location["country_code"]]:
                    self.spikes_by_region[e["game_id"]][location["country_code"]][division] = []
                
                self.spikes_by_region[e["game_id"]][location["country_code"]][division].append(e)
    

    def run(self):
        logger.info('Starting shared anomaly processing')

        all_spikes = self.db_controller.get_all_spikes()
        self.divide_spikes_by_region(all_spikes)

        users_by_region = get_users_by_region(self.users_locations)
        data_to_process = []

        for game, spikes_by_game in self.spikes_by_region.items():
            for country, spikes_per_country in spikes_by_game.items():
                for division, spikes in spikes_per_country.items():
                    spikes_chunks = [spikes[i:i + chunk_size] for i in range(0, len(spikes), chunk_size)]
                    for chunk in spikes_chunks:
                            data_to_process.append({"game_id": game, "country": country, "region": division, "spikes": chunk})

        shuffle(data_to_process)
        data_chunks = [{"data": list(c), "idx": idx} for idx, c in enumerate(mit.divide(number_cores, data_to_process))]

        pool = multiprocessing.Pool(number_cores)
        tmp = partial(process_game, users_by_region)
        
        results = pool.imap(func=tmp, iterable=data_chunks)

        for r in results:
            pass
                        
        self.logger.info('Finished shared anomaly processing')


if __name__ == "__main__":
    processor = SharedAnomalyDetection()
    processor.run()