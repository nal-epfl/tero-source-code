import sys
import json
import redis
import multiprocessing
import math
import time

import more_itertools as mit

from functools import partial
from datetime import timedelta, datetime
from pymongo import DESCENDING, MongoClient
from config import redis_host, redis_port, redis_password, mongo_host, mongo_password, mongo_port, mongo_user, grouping_window_size
from utils.utils import QoEBandProcess, get_users_by_region, get_biggest_division, get_stored_locations
from utils.logger import get_logger

from db.mongo_controller import MongoController

class DatetimeEncoder(json.JSONEncoder):
    def default(self, o):
        return o.strftime("%Y-%m-%d-%H-%M-%S")


number_cores = 10
logger = get_logger("online_spikes")


class OnlineSpikeDetector:
    def __init__(self):
        self.db_controller = MongoController()
        

    def get_spikes(self, game_id, user_id, datapoints):
        if not datapoints:
            return {"game_id": game_id, "user_id": user_id, "spikes": [], "spikes_to_remove": []}

        datapoints.sort(key=lambda x: x["date"])
        qoe_seq = QoEBandProcess(self.db_controller.qoe_band.get(game_id, 15))

        new_sequence, _ = qoe_seq.get_sequence_qoe_groups(datapoints)
        
        spikes_to_remove = []
        spikes = []

        latest_qoe_sequence = [x for x in self.db_controller.mongo_client.processed.qoe.find({"game_id": game_id, "user_id": user_id, "start": {"$lte": datapoints[0]["date"].timestamp()}}).sort("start", direction=DESCENDING).limit(1)]
        latest_stable = [x for x in self.db_controller.mongo_client.processed.qoe.find({"game_id": game_id, "user_id": user_id, "start": {"$lte": datapoints[0]["date"].timestamp()}, "stable": True}).sort("start", direction=DESCENDING).limit(1)]

        if not latest_qoe_sequence or not latest_stable:
            return {"game_id": game_id, "user_id": user_id, "spikes": [], "spikes_to_remove": []}

        latest_qoe_sequence = latest_qoe_sequence[0]
        latest_stable = latest_stable[0]
        
        for new_seq in new_sequence:
            latency = [x["latency"] for x in new_seq]
            new_min = min(latency)
            new_max = max(latency)

            to_insert = False
            
            if latest_qoe_sequence:
                # Alternative 1: New points continue latest sequence
                
                if max(new_max, latest_qoe_sequence["max"]) - min(new_min, latest_qoe_sequence["min"]) <= self.db_controller.qoe_band.get(game_id, 15):                
                    old_end = latest_qoe_sequence["end"]                    
                    
                    latest_qoe_sequence["min"] = min(new_min, latest_qoe_sequence["min"])
                    latest_qoe_sequence["max"] = max(new_max, latest_qoe_sequence["max"])
                    latest_qoe_sequence["end"] = new_seq[-1]["date"].timestamp()

                    latest_qoe_sequence["length"] += len([x for x in new_seq if old_end < x["date"].timestamp() <= latest_qoe_sequence["end"]])
                    latest_qoe_sequence["stable"] = timedelta(minutes=self.db_controller.stable_length.get(game_id, 40)) < datetime.fromtimestamp(latest_qoe_sequence["end"]) - datetime.fromtimestamp(latest_qoe_sequence["start"])
                    
                    self.db_controller.mongo_client.processed.qoe.update_one({'_id':latest_qoe_sequence["_id"]}, {"$set": latest_qoe_sequence}, upsert=False)

                    if timedelta(minutes=self.db_controller.stable_length.get(game_id, 40)) < datetime.fromtimestamp(latest_qoe_sequence["end"]) - datetime.fromtimestamp(latest_qoe_sequence["start"]):
                        # Do not delete, just mark as "not spike" and when that was decided.
                        spikes_to_remove.append({"game_id": game_id, "user_id": user_id, "start": latest_qoe_sequence["start"], "end": latest_qoe_sequence["end"], "timestamp": datetime.now()})
                else:
                    to_insert = True


                if (to_insert and timedelta(minutes=self.db_controller.stable_length.get(game_id, 40)) > new_seq[-1]["date"] - new_seq[0]["date"]) or \
                   (not to_insert and timedelta(minutes=self.db_controller.stable_length.get(game_id, 40)) > datetime.fromtimestamp(latest_qoe_sequence["end"]) - datetime.fromtimestamp(latest_qoe_sequence["start"])):
                    for point in new_seq:
                        if point["latency"] > latest_stable["max"] and abs(point["latency"] - latest_stable["min"]) > self.db_controller.qoe_band.get(game_id, 15):   
                            spikes.append(point)
       
            else:
                to_insert = True
            
            if to_insert:
                new_latest = {"game_id": game_id, "user_id": user_id, "seq_idx": latest_qoe_sequence["seq_idx"]+1 if latest_qoe_sequence else 0, "min": new_min, "max": new_max, "start": new_seq[0]["date"].timestamp(), 
                    "end": new_seq[-1]["date"].timestamp(), "length": len(new_seq)}       
                new_latest["stable"] = timedelta(minutes=self.db_controller.stable_length.get(game_id, 40)) < datetime.fromtimestamp(new_latest["end"]) - datetime.fromtimestamp(new_latest["start"])

                self.db_controller.mongo_client.processed.qoe.insert_one(new_latest)

                latest_qoe_sequence = new_latest
        
        return {"game_id": game_id, "user_id": user_id, "spikes": [{**x, "date": x["date"].timestamp()} for x in spikes], "spikes_to_remove": spikes_to_remove}



def get_by_user_game(data):
    detector = OnlineSpikeDetector()
    
    to_return = []
    
    for d in data:
        to_return.append(detector.get_spikes(d["game_id"], d["user_id"], d["latency"]))

    return to_return
       

def get_latency_by_game_user(all_latency):
    by_user_game = {}
    
    for l in all_latency:
        if l["game_id"] not in by_user_game:
            by_user_game[l["game_id"]] = {}

        if l["user_id"] not in by_user_game[l["game_id"]]:
            by_user_game[l["game_id"]][l["user_id"]] = []
        
        by_user_game[l["game_id"]][l["user_id"]].append(l)

    to_return = []
    for game_id, users in by_user_game.items():
        for user, latency in users.items():
            to_return.append({"game_id": game_id, "user_id": user, "latency": latency})

    return to_return


class SpikeCorrelation:
    def __init__(self, users_by_region):
        self.db_controller = MongoController()
        self.users_by_region = users_by_region

    def get_on_going_spikes(self, game_id, country, region, start_date, end_date):
        future_spikes = []
        
        for x in self.db_controller.mongo_client.processed.on_going.find({"game_id": game_id, "country": country, "region": region, "spike.date": {"$gte": start_date.timestamp()}}, projection={"_id": False}).sort("date"):
            if end_date.timestamp() < x["spike"]["date"]:
                return future_spikes
            future_spikes.append(x)

        return future_spikes

    def correlate(self, game_id, country, region, region_spikes):
        window_td = (timedelta(minutes=grouping_window_size)/2)

        parameters = self.db_controller.mongo_client.processed.parameters.find_one({"game_id": game_id, "country": country, "region": region})

        if not parameters or parameters["significance"] <= 10:
            return
                
        for spike in region_spikes:
            spike_date = datetime.fromtimestamp(spike["date"])

            future_spikes = self.get_on_going_spikes(game_id, country, region, spike_date, spike_date+window_td)
            future_date = datetime.fromtimestamp(future_spikes[-1]["spike"]["date"])

            other_spikes = self.get_on_going_spikes(game_id, country, region, future_date-timedelta(minutes=grouping_window_size), future_date)

            other_streams = [x for x in self.db_controller.mongo_client.data.latency.find({"game_id": game_id, "user_id": {"$in": self.users_by_region[country][region]}, 
                                                                        "date": {"$gte": (future_date - timedelta(minutes=grouping_window_size)).timestamp(), "$lte": future_date.timestamp()}}, projection={"_id": False})]             
            
            N = len(set([*[x["user_id"] for x in other_streams], *[x["spike"]["user_id"] for x in other_spikes]]))
            D = len(set([x["spike"]["user_id"] for x in other_spikes]))
            
            probability = math.comb(N, D) * parameters["p_e"]**D  * (1 - parameters["p_e"])**(N - D)
            under_threshold = probability < 0.01/100

            logger.info("Independent probability: {}".format(probability))

            if under_threshold:
                self.db_controller.mongo_client.processed.correlated.insert_one({"time": datetime.now(), "results": {"p_d": parameters["p_e"], "N": N, "D": D, "prob": probability}, "spike": spike, 
                                                                                 "country": country, "region": region, "other_anomalies": other_spikes, "all_overlaps": other_streams})
                                

def run_spike_correlation(users_by_region, data):
    correlation = SpikeCorrelation(users_by_region)

    for d in data:
        correlation.correlate(d["game_id"], d["country"], d["region"], d["spikes"])


def main():
    cache = redis.Redis(host=redis_host, port=redis_port, password=redis_password)
    
    new_latency = []
    for x in cache.spop("new_latency", count=cache.scard("new_latency")):
        data = json.loads(x.decode("utf-8"))
        new_latency.append({**data, "date": datetime.fromtimestamp(data["date"]), "latency": int(data["latency"])})

    if not new_latency:
        logger.info("Nothing to process")
        sys.exit(1)

    logger.info("Found {} new latency entries".format(len(new_latency)))
    
    latency = get_latency_by_game_user(new_latency)
    latency_chunks = [list(c) for c in mit.divide(number_cores, latency)]

    pool = multiprocessing.Pool(number_cores)
    results = pool.imap(func=get_by_user_game, iterable=latency_chunks)

    mongo_client = MongoClient("mongodb://{}:{}/".format(mongo_host, mongo_port), username=mongo_user, password=mongo_password)
    
    users_locations = get_stored_locations(mongo_client)
    users_by_region = get_users_by_region(users_locations)

    spikes_by_game = {}
    useful_spikes = {}
    all_spikes = {}
    spikes_to_insert = []
    spikes_to_remove = []

    for batch_results in results:
        for spikes in batch_results:
            if spikes["spikes"]: 
                if spikes["game_id"] not in all_spikes:
                    all_spikes[spikes["game_id"]] = 0            
                all_spikes[spikes["game_id"]] += len(spikes["spikes"])

                location = users_locations.get(spikes["user_id"], {})
                division, _ = get_biggest_division(location)

                if not division:
                    for e in spikes["spikes"]:
                        spikes_to_insert.append({"game_id": spikes["game_id"], "spike": e, "time_detected": datetime.now().timestamp()})

                    continue

                if spikes["game_id"] not in useful_spikes:
                    useful_spikes[spikes["game_id"]] = 0
                useful_spikes[spikes["game_id"]] += len(spikes["spikes"])

                if spikes["game_id"] not in spikes_by_game:
                    spikes_by_game[spikes["game_id"]] = {}

                if location["country_code"] not in spikes_by_game[spikes["game_id"]]:
                    spikes_by_game[spikes["game_id"]][location["country_code"]] = {}
                    
                if division not in spikes_by_game[spikes["game_id"]][location["country_code"]]:
                    spikes_by_game[spikes["game_id"]][location["country_code"]][division] = []
                    
                spikes_by_game[spikes["game_id"]][location["country_code"]][division].extend(spikes["spikes"])
                
                for e in spikes["spikes"]:
                    spikes_to_insert.append({"game_id": spikes["game_id"], "country": location["country_code"], "region": division, "spike": e, "time_detected": datetime.now().timestamp()})

            if spikes["spikes_to_remove"]:
                spikes_to_remove.extend(spikes["spikes_to_remove"])

    if spikes_to_insert:
        mongo_client.processed.on_going.insert_many(spikes_to_insert)

    logger.info("Total number of spikes: {}".format(all_spikes))
    logger.info("Total number of spikes with known region: {}".format(useful_spikes))

    to_correlate = []

    for game_id, spikes_by_region in spikes_by_game.items():
        for country, spikes_per_region in spikes_by_region.items():
            for region, region_spikes in spikes_per_region.items():
                if not region_spikes:
                    continue

                to_correlate.append({"game_id": game_id, "country": country, "region": region, "spikes": region_spikes})

    to_correlate_chunks = [list(c) for c in mit.divide(number_cores, to_correlate)]

    pool = multiprocessing.Pool(number_cores)
    tmp = partial(run_spike_correlation, users_by_region)
    results = pool.imap(func=tmp, iterable=to_correlate_chunks)                
   
    for sequence in spikes_to_remove:
        for spike in mongo_client.processed.on_going.find({"game_id": sequence["game_id"], "spike.user_id": sequence["user_id"], "spike.date": {"$gte": sequence["start"]}}):
            if spike["spike"]["date"] < sequence["end"]:
                break
            
            spike["correction_time"] = sequence["timestamp"]
            _id = spike.pop("_id")
            mongo_client.processed.on_going.update_one({'_id': _id}, {"$set": spike}, upsert=False)
        

    for r in results:
        pass

    logger.info("Finishing process...")


if __name__ =="__main__":
    main()