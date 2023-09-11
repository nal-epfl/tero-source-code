import json
import redis
import multiprocessing

from functools import partial
from db.mongo_controller import MongoController
from datetime import datetime, timedelta
from utils.utils import group_spike_list
from config import grouping_window_size, redis_host, redis_password, redis_port


class SequenceGrouper:
    def __init__(self):
        self.db_controller = MongoController()
        

    def group_exiting_user_sequences(self, old_groups, new_sequences, qoe_band):
        if not new_sequences:
            return []

        grouped = old_groups
        relevant_length = 0
        for old in old_groups:
            for s in old["sequences"]:
                if "is_spike" not in s and s["stable"]:
                    relevant_length += s["length"]

        for s in new_sequences:
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
    

    def group_new_user_sequences(self, sequences, qoe_band):
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


    def remove_changed_subsequences(self, old_entry, since_date, last_start):
        grouped = []

        for group in old_entry["grouped_sequences"]:
            if not group["sequences"]:
                continue
            
            sequences = []
            min_boundary = group["sequences"][0]["min"]
            max_boundary = group["sequences"][0]["max"]
            
            for seq in group["sequences"]:
                if seq["end"] < since_date and seq["start"] < last_start:
                    sequences.append(seq)

                    if seq["min"] < min_boundary:
                        min_boundary = seq["min"]

                    if seq["max"] > max_boundary:
                        max_boundary = seq["max"]

            if sequences:
                grouped.append({"min": min_boundary, "max": max_boundary, "sequences": sequences})
        
        return grouped

    def get_last_grouped_sequence(self, old_entry):
        last_start = old_entry["grouped_sequences"][0]["sequences"][0]["start"]
                
        for group in old_entry["grouped_sequences"]:
            if not group["sequences"]:
                continue   
            
            for seq in group["sequences"]:
                if seq["start"] > last_start:
                    last_start = seq["start"]   
                    
        return last_start


    def process_user(self, user_data):
        user_to_recluster = False
        qoe_band = self.db_controller.qoe_band.get(user_data["game_id"], 15)

        old_sequences = self.db_controller.get_grouped_sequences(user_data["game_id"], user_data["user_id"])

        if old_sequences:
            last_start = self.get_last_grouped_sequence(old_sequences)
            filtered_seqs = self.remove_changed_subsequences(old_sequences, user_data["since"], last_start)

            if filtered_seqs:
                sequences = self.db_controller.get_affected_sequences(user_data["game_id"], user_data["user_id"], min(user_data["since"], last_start))
                grouped_sequences = self.group_exiting_user_sequences(filtered_seqs, sequences, qoe_band)
            else:
                sequences = self.db_controller.get_affected_sequences(user_data["game_id"], user_data["user_id"], user_data["since"])
                grouped_sequences = self.group_new_user_sequences(sequences, qoe_band)
                user_to_recluster = True
        else:
            sequences = self.db_controller.get_affected_sequences(user_data["game_id"], user_data["user_id"], user_data["since"])
            grouped_sequences = self.group_new_user_sequences(sequences, qoe_band)
            user_to_recluster = True

        if grouped_sequences:
            if old_sequences:
                last_main_sequence = sorted(old_sequences["grouped_sequences"], key=lambda x: x["proportion"], reverse=True)[0]
                current_main_sequence = sorted(grouped_sequences, key=lambda x: x["proportion"], reverse=True)[0]

                if not (max(current_main_sequence["max"], last_main_sequence["max"]) - min(current_main_sequence["min"], last_main_sequence["min"]) <= qoe_band):
                    user_to_recluster = True

            to_insert = {"game_id": user_data["game_id"], "user_id": user_data["user_id"], "grouped_sequences": grouped_sequences}
            self.db_controller.update_grouped_sequences(to_insert)
        
        return user_to_recluster

            
def process_users(users):
    users_to_recluster = set()
    grouper = SequenceGrouper()
    
    for user in users:
        should_recluster = grouper.process_user(user)
        if should_recluster:
            users_to_recluster.add(tuple([user["game_id"], user["user_id"]]))

    return users, users_to_recluster
        

class ResultsGrouper:
    def __init__(self, logger):
        self.start_range = datetime(year=2021, month=5, day=24).timestamp()
        self.db_controller = MongoController(self.start_range, False)
        self.logger = logger
        self.cache = redis.Redis(host=redis_host, port=redis_port, password=redis_password)


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
    

    def get_to_process(self):
        # log_entries = [json.loads(x.decode("utf8")) for x in self.cache.smembers("new_to_group")]
        log_entries = [json.loads(x.decode("utf8")) for x in self.cache.spop("new_to_group", count=self.cache.scard("new_to_group"))]
        
        entries_user_game = {}

        for entry in log_entries:
            if tuple([entry["user_id"], entry["game_id"]]) not in entries_user_game:
                entries_user_game[tuple([entry["user_id"], entry["game_id"]])] = []

            entries_user_game[tuple([entry["user_id"], entry["game_id"]])].append(entry["date"])

        return [{"user_id": x[0], "game_id": x[1], "since": min(k)} for x, k in entries_user_game.items()]


    def group_sequences(self):
        chunk_size = 100        
        number_cores = 5

        to_process = self.get_to_process()
        self.logger.info("Found {} pairs to process".format(len(to_process)))        

        users_chunks = [to_process[i:i + chunk_size] for i in range(0, len(to_process), chunk_size)]

        pool = multiprocessing.Pool(number_cores)
        results = pool.imap(func=process_users, iterable=users_chunks)

        users_to_recluster = set()
        ready = 0
        for r in results:
            ready += len(r[0])
            users_to_recluster.update(r[1])

            self.logger.info("Users finished: {}/{}".format(ready, len(to_process)))

        self.logger.info("Inserting users to recluster")
        for x in users_to_recluster:
            self.cache.sadd("new_to_cluster", json.dumps(x))

        self.logger.info("Finished grouping sequences")

