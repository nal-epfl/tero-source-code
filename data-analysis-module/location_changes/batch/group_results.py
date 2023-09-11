import multiprocessing

from functools import partial
from db.mongo_controller import MongoController
from datetime import datetime, timedelta
from utils.utils import group_spike_list
from config import grouping_window_size

from utils.logger import get_logger


class SequenceGrouper:
    def __init__(self):
        self.db_controller = MongoController()
        

    def group_user_sequences(self, sequences, qoe_band):
        first_seq = None
        for x in sequences:
            first_seq = x
            break
        
        if not first_seq:
            return []

        grouped = [{"min": first_seq["min"], "max": first_seq["max"], "sequences": [first_seq]}]
        relevant_length = 0
        if "is_spike" not in first_seq and first_seq["stable"]:
            relevant_length += first_seq["length"]

        for s in sequences:
            found = False

            if "is_spike" not in s and s["stable"]:
                relevant_length += s["length"]

            for group in grouped:
                if group["min"] <= s["min"] and s["max"] <= group["max"]:
                    group["sequences"].append(s)
                    found = True
                    break
            
            if not found:
                grouped.append({"min": s["min"], "max": s["max"], "sequences": [s]})

        grouped_sequences = [{"min": grouped[0]["min"], "max": grouped[0]["max"], "sequences": grouped[0]["sequences"]}]

        for g in grouped[1:]:
            found = False

            for group in grouped_sequences:
                min_boundary = min(g["min"], group["min"])
                max_boundary = max(g["max"], group["max"])

                if max_boundary - min_boundary <= qoe_band:
                    group["min"] = min_boundary
                    group["max"] = max_boundary
                    group["sequences"].extend(g["sequences"])
                    found = True
                    break
            
            if not found:
                grouped_sequences.append(g)

        relevant_length = relevant_length if relevant_length else 1

        for group in grouped_sequences:
            group["proportion"] = sum([x["length"] for x in group["sequences"] if "is_spike" not in x and x["stable"]])/relevant_length

        return grouped_sequences


    def process_user(self, user_id):
        to_insert = []

        for game in self.db_controller.get_all_games():               
            sequences = self.db_controller.get_all_sequences(game, user_id)
            qoe_band = self.db_controller.qoe_band.get(game, 15)

            grouped_sequences = self.group_user_sequences(sequences, qoe_band)
            if grouped_sequences:
                to_insert.append({"game_id": game, "user_id": user_id, "grouped_sequences": grouped_sequences})

        if to_insert:
            self.db_controller.store_grouped_sequences(to_insert)


def process_users(users):
    grouper = SequenceGrouper()
    
    for user in users:
        grouper.process_user(user)

    return users
        

class ResultsGrouper:
    def __init__(self, logger):
        self.start_range = datetime(year=2021, month=5, day=24).timestamp()
        self.db_controller = MongoController(self.start_range, False)
        self.logger = logger


    def group_spikes(self):
        self.logger.info("Cleaning previous grouped spikes...")
        self.db_controller.clean_grouped_spikes()

        self.logger.info("Getting all users with spikes...")
        users = self.db_controller.get_users_with_spikes()
        self.logger.info("Found {} users to process.".format(len(users)))

        for user_id in users:        
            grouped_by_stream = {}
            
            for x in self.db_controller.get_spikes(user_id):     
                if x["game_id"] not in grouped_by_stream:
                    grouped_by_stream[x["game_id"]] = {}

                if x["stream_id"] not in grouped_by_stream[x["game_id"]]:
                    grouped_by_stream[x["game_id"]][x["stream_id"]] = []
                
                grouped_by_stream[x["game_id"]][x["stream_id"]].append(x)

            grouped_spikes = []
            more_than_one = {}

            for game_id, by_stream in grouped_by_stream.items():
                for stream_id, spikes in by_stream.items():
                    if len(spikes) > 1:
                        if game_id not in more_than_one:
                            more_than_one[game_id] = {}
                    
                        more_than_one[game_id][stream_id] = sorted(spikes, key=lambda x: x["date"])
                    else:
                        date = spikes[0].pop("date")
                        latency = spikes[0]["latency"]
                        grouped_spikes.append({**spikes[0], "start": date, "end": date, "latency": [{"latency": latency, "date": date}]})       

            for game_id, streams in more_than_one.items():
                for stream_id, spikes in streams.items():
                    spike = spikes[0]
                    grouped_spike = [spike]

                    for e in spikes[1:]:
                        if datetime.fromtimestamp(e["date"]) - datetime.fromtimestamp(grouped_spike[-1]["date"]) < timedelta(minutes=grouping_window_size):
                            grouped_spike.append(e)
                        else:
                            grouped_spikes.append(group_spike_list(grouped_spike))
                            grouped_spike = [e]

                        spike = e
                    
                    grouped_spikes.append(group_spike_list(grouped_spike))

            self.db_controller.store_grouped_spikes(grouped_spikes)

        self.logger.info("Indexing grouped spikes")
        self.db_controller.index_grouped_spikes()
        self.logger.info("Finished grouping spikes")
    


    def group_sequences(self):
        chunk_size = 1000        
        number_cores = 5

        self.logger.info("Cleaning previous grouped sequences...")
        self.db_controller.clean_grouped_sequences()

        users = self.db_controller.get_all_users()
        self.logger.info("Users with data: {}".format(len(users)))
        
        users_chunks = [users[i:i + chunk_size] for i in range(0, len(users), chunk_size)]

        pool = multiprocessing.Pool(number_cores)
        results = pool.imap(func=process_users, iterable=users_chunks)

        ready = 0
        for r in results:
            ready += len(r)
            self.logger.info("Users finished: {}/{}".format(ready, len(users)))

        self.logger.info("Indexing grouped sequences")
        self.db_controller.index_grouped_sequences()
        self.logger.info("Finished grouping sequences")



if __name__ == "__main__":
    logger = get_logger("group_results")

    processor = ResultsGrouper(logger)
    
    processor.group_spikes()
    processor.group_sequences()