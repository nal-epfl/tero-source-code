import redis
import json

from datetime import datetime
from db.db_controller import DBController
from pymongo import MongoClient, ReturnDocument, DESCENDING
from config import mongo_host, mongo_port, redis_host, redis_port, redis_password, mongo_user, mongo_password, qoe_bands, stable_period_min_length, stable_share


class MongoController(DBController):
    def __init__(self, since_date=datetime(year=2021, month=5, day=24).timestamp(), empty=False):
        super().__init__(since_date)
        self.empty = empty
        self.mongo_client = MongoClient("mongodb://{}:{}/".format(mongo_host, mongo_port), username=mongo_user, password=mongo_password)
        self.cache = redis.Redis(host=redis_host, port=redis_port, password=redis_password)
        
        self.qoe_band = qoe_bands
        self.stable_length = stable_period_min_length
        self.stable_min_share = stable_share


    def get_alternative(self, user_id, game_id, date):
        return self.mongo_client.data.alternative_values.find_one({"user_id": user_id, "game_id": game_id, "date": date}, projection={"_id": False})


    def get_all_alternatives(self, user_id, game_id):
        alternatives = {}
        for x in self.mongo_client.data.alternative_values.find({"user_id": user_id, "game_id": game_id, "date": {"$gte": self.since_date}}, projection={"_id": False}):
            alternatives[x["date"]] = x

        return alternatives


    def get_all_games(self):
        return ["295590", "116088", "135305", "118849", "464426", "273486", "319965", "747108", "314852", "461764", "742409", "762836", "128974", "970338", "614266", "101342", "452439"]
        

    def get_log_entries(self):
        now = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

        logs_to_process = [json.loads(x.decode("utf8")) for x in self.cache.spop("logs_latency", count=self.cache.scard("logs_latency"))]
        for log in logs_to_process:
            self.cache.sadd("to_publish", json.dumps(log))
            self.cache.sadd("new_to_group", json.dumps(log))
            self.cache.sadd("new_location_changes", json.dumps(log))

        return logs_to_process
        

    def get_all_users(self):
        return self.mongo_client.data.latency.distinct("user_id")


    def get_all_latency(self, user_id, game_id=None, since=None):
        if game_id:
            query = {"game_id": game_id, "user_id": user_id}
        else:
            query = {"user_id": user_id}    
        
        if since:
            query["date"] = {"$gte": since}
        else:
            query["date"] = {"$gte": self.since_date}

        return self.mongo_client.data.latency.find(query, projection={"_id": False})
    

    def store_latency(self, to_store):
        if self.empty:
            if to_store:
                self.mongo_client.processed.latency.insert_many(to_store)
        else:
            try:                
                for ts in to_store:
                    if "old" in ts:
                        continue
                    base_point_data = {"game_id": ts["game_id"], "user_id": ts["user_id"], "stream_id": ts["stream_id"], "date": ts["date"]}
                    self.mongo_client.processed.latency.find_one_and_replace(base_point_data, ts, upsert=True, return_document=ReturnDocument.AFTER)
            except Exception as e:
                print(e)

    def store_discarded_latency(self, to_store):
        if self.empty:
            if to_store:
                self.mongo_client.processed.discarded_latency.insert_many(to_store)
        else:
            try:                
                for ts in to_store:
                    if "old" in ts:
                        continue
                    base_point_data = {"game_id": ts["game_id"], "user_id": ts["user_id"], "stream_id": ts["stream_id"], "date": ts["date"]}
                    self.mongo_client.processed.discarded_latency.find_one_and_replace(base_point_data, ts, upsert=True, return_document=ReturnDocument.AFTER)
            except Exception as e:
                print(e)


    def store_glitches(self,to_store):
        if self.empty:
            if to_store:
                self.mongo_client.processed.glitches.insert_many(to_store)
        else:
            new_glitches = set()
            
            for ts in to_store:
                base_point_data = {"game_id": ts["game_id"], "user_id": ts["user_id"], "stream_id": ts["stream_id"], "date": ts["date"]}
                update_result = self.mongo_client.processed.glitches.find_one_and_replace(base_point_data, ts, upsert=True, return_document=ReturnDocument.AFTER)
                new_glitches.add(update_result["_id"])

                self.mongo_client.processed.latency.delete_one(base_point_data)    


    def store_spikes(self, to_store, old_spikes=set()):
        if self.empty:
            if to_store:
                self.mongo_client.processed.spikes.insert_many(to_store)
        else:
            if not to_store:
                return
            
            to_store_as_dict = {}
            to_store_as_set = set()
            for ts in to_store:
                to_store_as_set.add(tuple([ts["stream_id"], ts["date"]]))
                to_store_as_dict[tuple([ts["stream_id"], ts["date"]])] = ts

            spikes_to_insert = to_store_as_set - old_spikes
            spikes_to_remove = old_spikes - to_store_as_set

            for ti in spikes_to_insert:
                ts = to_store_as_dict[ti]

                base_point_data = {"game_id": ts["game_id"], "user_id": ts["user_id"], "stream_id": ts["stream_id"], "date": ts["date"]}
                
                to_insert = {**ts, "date": ts["date"].timestamp() if isinstance(ts["date"], datetime) else ts["date"]}
                self.mongo_client.processed.spikes.find_one_and_replace(base_point_data, to_insert, upsert=True, return_document=ReturnDocument.AFTER)

            user_id = to_store[0]["user_id"]
            game_id = to_store[0]["game_id"]

            for tr in spikes_to_remove:
                base_point_data = {"game_id": game_id, "user_id": user_id, "stream_id": tr[0], "date": tr[1]}
                self.mongo_client.processed.spikes.delete_one(base_point_data)    


    def store_discarded_spikes(self, to_store):
        if self.empty:
            if to_store:
                self.mongo_client.processed.discarded_spikes.insert_many(to_store)
        else:
            try:                
                for ts in to_store:
                    if "old" in ts:
                        continue
                    base_point_data = {"game_id": ts["game_id"], "user_id": ts["user_id"], "stream_id": ts["stream_id"], "date": ts["date"]}
                    self.mongo_client.processed.discarded_spikes.find_one_and_replace(base_point_data, ts, upsert=True, return_document=ReturnDocument.AFTER)
            except Exception as e:
                print(e)


    def store_qoe_sequences(self, to_store):
        if self.empty:
            if to_store:
                self.mongo_client.processed.qoe.insert_many(to_store)
        else:
            if not to_store:
                return

            game_id = to_store[0]["game_id"]
            user_id = to_store[0]["user_id"]

            last_stored = -1            
            for ts in to_store:
                base_sequence_data = {"game_id": ts["game_id"], "user_id": ts["user_id"], "seq_idx": ts["seq_idx"]}
                self.mongo_client.processed.qoe.find_one_and_replace(base_sequence_data, ts, upsert=True, return_document=ReturnDocument.AFTER)
                last_stored = max(last_stored, ts["seq_idx"])

            if last_stored >=0:
                 self.mongo_client.processed.qoe.delete_many({"game_id": game_id, "user_id": user_id, "seq_idx": {"$gt": last_stored}})


    def store_locations(self, to_store):
        self.mongo_client.processed.locations.drop()

        keys = list(to_store.keys())

        n_batches = 10
        batches_size = int(len(keys) / n_batches)

        keys_chunks = [keys[i:i + batches_size] for i in range(0, len(keys), batches_size)]

        for chunk in keys_chunks:
            to_insert_chunk = {}
            for k in chunk:
                to_insert_chunk[k] = to_store[k]

            self.mongo_client.processed.locations.insert_one(to_insert_chunk)

    
    def close(self):
        self.mongo_client.close()


    def store_parameters(self, parameters_to_store):                
        for pts in parameters_to_store:
            self.mongo_client.processed.parameters.find_one_and_replace({"game_id": pts["game_id"], "country": pts["country"], "region": pts["region"]}, pts, upsert=True)

    
    def get_spikes(self, user_id, game_id=None):
        if game_id:
            return self.mongo_client.processed.spikes.find({"user_id": user_id, "game_id": game_id}, projection={"_id": False}).sort("date")

        return self.mongo_client.processed.spikes.find({"user_id": user_id}, projection={"_id": False}).sort("date")
    

    def get_all_spikes(self):
        return self.mongo_client.processed.spikes.find(projection={"_id": False})
    
    
    def count_latency_points(self, user_id, game_id=None):
        if game_id:
            return self.mongo_client.processed.latency.count_documents({"game_id": game_id, "user_id": user_id})

        return self.mongo_client.processed.latency.count_documents({"user_id": user_id})

    
    def get_users_with_spikes(self):
        return self.mongo_client.processed.spikes.distinct("user_id")

    
    def get_parameters(self, game_id, country, region):
        return self.mongo_client.processed.parameters.find({"game_id": game_id, "country": country, "region": region})

    
    def store_shared_anomaly(self, general_info, detailed_info):
        _id = self.mongo_client.shared_anomalies.shared_anomaly.insert_one(general_info)        
        self.mongo_client.shared_anomalies.shared_anomaly_details.insert_one({"shared_anomaly": str(_id.inserted_id), **detailed_info})

   

    def get_spikes_in_range(self, game_id, start, end, users_list):    
        spikes_in_range = []
        for user in users_list:
            for x in self.mongo_client.processed.spikes.find({"game_id": game_id, "user_id": user, "date": {"$gte": start.timestamp()}}, projection={"_id": False}).sort("date"):
                if end.timestamp() < x["date"]:
                    break    
                spikes_in_range.append(x)
            
        return spikes_in_range


    def get_latency_in_range(self, game_id, start, end, users_list):
        latency_in_range = []
        for user in users_list:
            for x in self.mongo_client.processed.latency.find({"game_id": game_id, "user_id": user, "date": {"$gte": start.timestamp()}}, projection={"_id": False}).sort("date"):
                if end.timestamp() < x["date"]:
                    break   
                latency_in_range.append(x)
            
        return latency_in_range



    def store_sequences(self, to_store):
        self.mongo_client.processed.sequences.insert_many(to_store)


    def clean_grouped_spikes(self):
        self.mongo_client.processed.grouped_spikes.drop()


    def store_grouped_spikes(self, grouped_spikes):
        self.mongo_client.processed.grouped_spikes.insert_many(grouped_spikes)      


    def index_grouped_spikes(self):
        self.mongo_client.processed.grouped_spikes.create_index("user_id")
        self.mongo_client.processed.grouped_spikes.create_index("game_id")


    def get_containing_sequence(self, point):
        return self.mongo_client.processed.qoe.find_one({"user_id": point["user_id"], "game_id": point["game_id"], "start": {"$lte": point["date"]}, "end": {"$gte": point["date"]}}, projection={"_id": False})


    def get_stable_neighbour_sequence(self, seq, reverse=False):
        if reverse:
            neighbours = self.mongo_client.processed.qoe.find({"user_id": seq["user_id"], "game_id": seq["game_id"], "seq_idx": {"$lt": seq["seq_idx"]}, "stable": True}, projection={"_id": False}).sort("seq_idx", DESCENDING)
        else:
            neighbours = self.mongo_client.processed.qoe.find({"user_id": seq["user_id"], "game_id": seq["game_id"], "seq_idx": {"$gt": seq["seq_idx"]}, "stable": True}, projection={"_id": False}).sort("seq_idx")

        for n in neighbours:
            return n
        
        return None
    
    def get_user_game_counts(self, game_id, user_id):   
        if self.empty:
            return 0, {}, False

        stats = self.mongo_client.processed.user_game_stats.find_one({"game_id": game_id, "user_id": user_id}, projection={"_id": False})

        if not stats:
            return 0, {}, False
    
        return stats["total_points"], stats["total_per_digit_number"], True


    def get_user_game_stable_boundaries(self, game_id, user_id):
        if self.empty:
            return [1000000, -1]

        stats = self.mongo_client.processed.user_game_stats.find_one({"game_id": game_id, "user_id": user_id}, projection={"_id": False})

        if not stats:
            return [1000000, -1]
        
        return stats["stable_boundaries"]
    
    
    def save_user_game_counts(self, game_id, user_id, total_points, total_per_digit_number, stable_boundaries):
        to_insert = {"game_id": game_id, "user_id": user_id, "total_points": total_points, "total_per_digit_number": total_per_digit_number, "stable_boundaries": stable_boundaries}
        if self.empty:
            self.mongo_client.processed.user_game_stats.insert_one(to_insert)
        else:
            self.mongo_client.processed.user_game_stats.find_one_and_replace({"game_id": game_id, "user_id": user_id}, to_insert, upsert=True)

    
    def get_last_sequence(self, game_id, user_id):
        if self.empty:
            return []

        last_sequence = [x for x in self.mongo_client.processed.qoe.find({"game_id": game_id, "user_id": user_id, "start": {"$lte": self.since_date}}).sort("start", DESCENDING).limit(1)]

        if not last_sequence:
            return []
        
        return last_sequence[0]


    def get_last_sequence_latency(self, game_id, user_id):
        last_sequence = self.get_last_sequence(game_id, user_id)        
        init_idx = -1

        if last_sequence:
            to_return = set()
            for x in self.mongo_client.processed.latency.find({"game_id": game_id, "user_id": user_id, "date": {"$gte": last_sequence["start"]}}, projection={"_id": False}).sort("date"):
                if init_idx < 0:
                    init_idx = x["idx"]

                to_return.add(tuple([datetime.fromtimestamp(x["date"]), x["stream_id"], x["idx"]]))
            
            return to_return, last_sequence, init_idx

        return [], None, 0
    
    def get_last_sequence_first_latency(self, game_id, user_id):
        last_sequence = self.get_last_sequence(game_id, user_id)        
        if last_sequence:
            to_return = [{**x, "date": datetime.fromtimestamp(x["date"]), "latency": int(x["latency"])} 
                        for x in self.mongo_client.processed.latency.find({"game_id": game_id, "user_id": user_id, "date": {"$gte": last_sequence["start"]}}, projection={"_id": False}).sort("date").limit(1)][0]            
            return to_return, last_sequence

        return None, None


    def get_old_periods(self, game_id, user_id):
        if self.empty:
            return [], False

        was_stable = False
        sequences = []
        for idx, x in enumerate(self.mongo_client.processed.qoe.find({"game_id": game_id, "user_id": user_id, "start": {"$lte": self.since_date}}).sort("start")):
            if idx != x["seq_idx"]:
                self.mongo_client.processed.qoe.update_one({'_id':x["_id"]}, {"$set": {"seq_idx": idx}}, upsert=False)
                x["seq_idx"] = idx

            sequences.append(x)
            was_stable = was_stable or x["stable"]

        if not sequences:
            return [], False
        
        # Return without the last sequence
        return sorted(sequences[:-1], key=lambda x: x["seq_idx"]), was_stable            
    

    def get_old_spikes(self, game_id, user_id, since=None):
        if self.empty:
            return set()

        if since:
            query = {"game_id": game_id, "user_id": user_id, "date": {"$gte": since}}
        else:
            query = {"game_id": game_id, "user_id": user_id, "date": {"$gte": self.since_date}}

        spikes = set()
        for x in self.mongo_client.processed.spikes.find(query):
            spikes.add(tuple([x["stream_id"], x["date"]]))
        
        return spikes


    def get_active_days(self, game_id, region_users):
        active_days = {}
        for user in region_users:
            user_active_days = self.mongo_client.processed.active_days.find_one({"game_id": game_id, "user_id": user})
            if user_active_days:
                for day in user_active_days["active_days"]:
                    if day not in active_days:
                        active_days[day] = []
                    active_days[day].append(user)
        
        return active_days
    

    def get_all_sequences(self, game_id, user_id):
        return self.mongo_client.processed.qoe.find({"game_id": game_id, "user_id": user_id}, projection={"_id": False}).sort("start")
    

    def get_affected_sequences(self, game_id, user_id, since_date):
        return self.mongo_client.processed.qoe.find({"game_id": game_id, "user_id": user_id, "end": {"$gte": since_date}}, projection={"_id": False}).sort("start")


    
    def clean_grouped_sequences(self):
        self.mongo_client.processed.grouped_sequences.drop()


    def store_grouped_sequences(self, sequences):
        self.mongo_client.processed.grouped_sequences.insert_many(sequences)


    def update_grouped_sequences(self, to_update):
        self.mongo_client.processed.grouped_sequences.find_one_and_replace({"game_id": to_update["game_id"], "user_id": to_update["user_id"]}, to_update, upsert=True)
        

    def index_grouped_sequences(self):
        self.mongo_client.processed.grouped_sequences.create_index("user_id")
        self.mongo_client.processed.grouped_sequences.create_index("game_id")


    def get_grouped_sequences(self, game_id, user_id):
        return self.mongo_client.processed.grouped_sequences.find_one({"game_id": game_id, "user_id": user_id}, projection={"_id": False})
        

    
    def store_active_days(self, game_id, user_id, active_days):
        to_insert = {"game_id": game_id, "user_id": user_id, "active_days": active_days}

        if self.empty:
            self.mongo_client.processed.active_days.insert_one({**to_insert, "active_days": list(to_insert["active_days"])})
        else:
            old_active_days = self.mongo_client.processed.active_days.find_one({"game_id": game_id, "user_id": user_id})

            if old_active_days:
                to_insert["active_days"] = set(to_insert["active_days"]).union(set(old_active_days["active_days"]))
            
            to_insert["active_days"] = list(to_insert["active_days"])
            self.mongo_client.processed.active_days.find_one_and_replace({"game_id": game_id, "user_id": user_id}, to_insert, upsert=True)
        
    
    def get_region_clusters(self, game_id, country, region):
        return [x for x in self.mongo_client.processed.clusters.find({"game_id": game_id, "country": country, "region": region})]