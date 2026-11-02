import multiprocessing

from functools import partial
from datetime import datetime, timedelta
from pymongo import MongoClient

from config import mongo_host, mongo_port, mongo_user, mongo_password, qoe_band, games

from logger import get_logger


class UserProcessor:
    def __init__(self):
        self.mongo_client = MongoClient("mongodb://{}:{}/".format(mongo_host, mongo_port), username=mongo_user, password=mongo_password)
        

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
        game_list = []

        for game in games:               
            sequences = self.mongo_client.thumbnails.qoe_sequences.find({"game_id": game, "user_id": user_id}, projection={"_id": False})

            grouped_sequences = self.group_user_sequences(sequences, qoe_band)
            if grouped_sequences:
                to_insert.append({"game_id": game, "user_id": user_id, "grouped_sequences": grouped_sequences})
                game_list.append(game)

        if to_insert:
            self.mongo_client.thumbnails.grouped_sequences.insert_many(to_insert)

        return game_list


def process_users(users):
    grouper = UserProcessor()
    
    for user in users:
        grouper.process_user(user)

    return users
        

class SequenceGrouper:
    def __init__(self, logger):
        self.mongo_client = MongoClient("mongodb://{}:{}/".format(mongo_host, mongo_port), username=mongo_user, password=mongo_password)
        self.logger = logger


    def group_sequences(self):
        chunk_size = 1000        
        number_cores = 5

        self.logger.info("Cleaning previous grouped sequences...")
        self.mongo_client.thumbnails.grouped_sequences.drop()

        users = list(set([x["user_id"] for x in self.mongo_client.thumbnails.latency_presence.find()]))
        
        self.logger.info("Users with data: {}".format(len(users)))
        
        users_chunks = [users[i:i + chunk_size] for i in range(0, len(users), chunk_size)]

        pool = multiprocessing.Pool(number_cores)
        results = pool.imap(func=process_users, iterable=users_chunks)

        ready = 0
        for r in results:
            ready += len(r)
            self.logger.info("Users finished: {}/{}".format(ready, len(users)))

        self.logger.info("Indexing grouped sequences")
        self.mongo_client.thumbnails.grouped_sequences.create_index("user_id")
        self.mongo_client.thumbnails.grouped_sequences.create_index("game_id")
        self.logger.info("Finished grouping sequences")



