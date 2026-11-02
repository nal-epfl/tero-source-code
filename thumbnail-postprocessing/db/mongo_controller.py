import redis
import json

from datetime import datetime, timedelta
from pymongo import MongoClient, ReturnDocument, DESCENDING
from config import mongo_host, mongo_port, redis_host, redis_port, redis_password, mongo_user, mongo_password, qoe_band, stable_period_min_length, stable_share



class MongoController:
    def __init__(self, since_date=datetime(year=2021, month=5, day=24).timestamp()):
        self.since_date = since_date
        self.mongo_client = MongoClient("mongodb://{}:{}/".format(mongo_host, mongo_port), username=mongo_user, password=mongo_password)
        self.cache = redis.Redis(host=redis_host, port=redis_port, password=redis_password)
        
        self.qoe_band = qoe_band
        self.stable_length = stable_period_min_length
        self.stable_min_share = stable_share

        self.latency_db = self.mongo_client.thumbnail_data
        self.alternatives_db = self.mongo_client.thumbnail_alternatives



    def get_alternative(self, user_id, game_id, date):
        date_dt = datetime.fromtimestamp(date)

        return self.alternatives_db["alternatives_{}_{:02d}".format(date_dt.year, date_dt.month)].find_one({"user_id": user_id, "game_id": game_id, "date": date}, projection={"_id": False})


    def get_all_alternatives(self, user_id, game_id):
        alternatives = {}
        
        user_presence = self.mongo_client.thumbnails.alternatives_presence.find_one({"user_id": user_id})
        if not user_presence:
            return alternatives

        for presence in user_presence["presence"]:
            for x in self.alternatives_db[presence].find({"user_id": user_id, "game_id": game_id}, projection={"_id": False}):
                alternatives[x["date"]] = x

        return alternatives
      

    def get_log_entries(self):
        logs_to_process = [json.loads(x.decode("utf8")) for x in self.cache.spop("new_latency", count=self.cache.scard("new_latency"))]
        # logs_to_process = [json.loads(x.decode("utf8")) for x in self.cache.smembers("new_latency")]

        return logs_to_process
           

    def get_all_users(self):
        return [x["user_id"] for x in self.mongo_client.thumbnails.latency_presence.find()]


    def get_starting_collection(self, collection_list, since):
        starting_collection_idx = 0
        if since:
            collection_list.sort()
                       
            since_dt = datetime.fromtimestamp(since)
            since_col = "latency_{}_{:02d}".format(since_dt.year, since_dt.month)

            for idx, col in enumerate(collection_list):
                if col >= since_col:
                    starting_collection_idx = idx
                    break

        return starting_collection_idx


    def remove_markers(self, point): 
        # Markers
        if "should_skip" in point:
            point.pop("should_skip")
        
        if "is_spike" in point:
            point.pop("is_spike")
        
        if "original_latency" in point:
            latency = point.pop("original_latency")
            point["latency"] = latency
        
        return point


    def get_all_latency(self, user_id, game_id=None, since=None):
        collections = []
        query = {}

        if game_id:
            query = {"game_id": game_id}
        
        if user_id:
            user_presence = self.mongo_client.thumbnails.latency_presence.find_one({"user_id": user_id})

            if not user_presence:
                return []
        
            collections = user_presence["presence"]            
            query["user_id"] = user_id
        else:
            collections = [collection for collection in self.mongo_client.thumbnail_data.list_collection_names()]
                    
        collections.sort()
        starting_collection_idx = 0

        if since:
            starting_collection_idx = self.get_starting_collection(collections, since)
        
        results = []
        for col in user_presence["presence"][starting_collection_idx:]:
            # Remove markers
            results.extend([self.remove_markers(x) for x in self.latency_db[col].find(query, projection={"_id": False})])

        return results


    def store_latency(self, to_store):
        streams = {}
        stream_to_user = {}

        try:                
            for ts in to_store:
                if "old" in ts:
                    ts.pop("old")

                if ts["stream_id"] not in streams:
                    streams[ts["stream_id"]] = {"start": ts["date"], "end": ts["date"]}
                
                streams[ts["stream_id"]]["start"] = min(streams[ts["stream_id"]]["start"], ts["date"])
                streams[ts["stream_id"]]["end"] = max(streams[ts["stream_id"]]["start"], ts["date"])
                
                stream_to_user[ts["stream_id"]] = ts["user_id"]

                # only modified latency needs to be inserted (spikes and points with original latency)
                # if "original_latency" not in ts and "is_spike" not in ts:
                #     continue

                base_point_data = {"game_id": ts["game_id"], "user_id": ts["user_id"], "stream_id": ts["stream_id"], "date": ts["date"]}
                                
                date = datetime.fromtimestamp(ts["date"])
                self.latency_db["latency_{}_{:02d}".format(date.year, date.month)].find_one_and_replace(base_point_data, ts, upsert=True, return_document=ReturnDocument.AFTER)
        except Exception as e:
            print(e)

        # Update stream metadata if necessary
        for stream_id, metadata in streams.items():
            self.mongo_client.thumbnails.stream_ends.find_one_and_replace({"stream_id": stream_id}, {"user_id": stream_to_user[stream_id], "stream_id": stream_id, **metadata}, upsert=True, return_document=ReturnDocument.AFTER)


    def store_glitches(self,to_store):        
        for ts in to_store:
            if "old" in ts:
                ts.pop("old")

            base_point_data = {"game_id": ts["game_id"], "user_id": ts["user_id"], "stream_id": ts["stream_id"], "date": ts["date"]}
            dt = datetime.fromtimestamp(ts["date"])
                        
            self.latency_db["latency_{}_{:02d}".format(dt.year, dt.month)].find_one_and_replace(base_point_data, {**ts, "should_skip": True}, upsert=True, return_document=ReturnDocument.AFTER)
            

    def store_spikes(self, to_store):
        if not to_store:
            return

        for ts in to_store:
            if "old" in ts:
                ts.pop("old")

            base_point_data = {"game_id": ts["game_id"], "user_id": ts["user_id"], "stream_id": ts["stream_id"], "date": ts["date"]}            
            to_insert = {**ts, "date": ts["date"].timestamp() if isinstance(ts["date"], datetime) else ts["date"], "is_spike": True}

            date = datetime.fromtimestamp(ts["date"])
        
            self.latency_db["latency_{}_{:02d}".format(date.year, date.month)].find_one_and_replace(base_point_data, to_insert, upsert=True, return_document=ReturnDocument.AFTER)    
            self.cache.sadd("streams_new_spikes", json.dumps({"game_id": ts["game_id"], "user_id": ts["user_id"], "stream_id": ts["stream_id"], "date": ts["date"], "latency": ts["latency"]}))

            self.mongo_client.thumbnails.spikes.find_one_and_replace(base_point_data, to_insert, upsert=True, return_document=ReturnDocument.AFTER)    


    def store_qoe_sequences(self, to_store):
        if not to_store:
            return

        game_id = to_store[0]["game_id"]
        user_id = to_store[0]["user_id"]

        last_stored = -1            
        for ts in to_store:
            base_sequence_data = {"game_id": ts["game_id"], "user_id": ts["user_id"], "seq_idx": ts["seq_idx"]}
            if self.mongo_client.thumbnails.qoe_sequences.count_documents(base_sequence_data, limit=1) > 0:
                # The qoe band already exists, so its limits  
                self.mongo_client.thumbnails.qoe_sequences.find_one_and_replace(base_sequence_data, ts, upsert=True, return_document=ReturnDocument.AFTER)
            else:
                # This is a new qoe band that needs to be inserted and analyzed
                self.cache.sadd("streams_new_qoe", json.dumps(ts))
                self.mongo_client.thumbnails.qoe_sequences.insert_one(ts)
                            
            last_stored = max(last_stored, ts["seq_idx"])

        if last_stored >=0:
            self.mongo_client.thumbnails.qoe_sequences.delete_many({"game_id": game_id, "user_id": user_id, "seq_idx": {"$gt": last_stored}})

    
    def get_containing_sequence(self, point):
        return self.mongo_client.thumbnails.qoe_sequences.find_one({"user_id": point["user_id"], "game_id": point["game_id"], "start": {"$lte": point["date"]}, "end": {"$gte": point["date"]}}, projection={"_id": False})


    def get_stable_neighbour_sequence(self, seq, reverse=False):
        if reverse:
            neighbours = self.mongo_client.thumbnails.qoe_sequences.find({"user_id": seq["user_id"], "game_id": seq["game_id"], "seq_idx": {"$lt": seq["seq_idx"]}, "stable": True}, projection={"_id": False}).sort("seq_idx", DESCENDING)
        else:
            neighbours = self.mongo_client.thumbnails.qoe_sequences.find({"user_id": seq["user_id"], "game_id": seq["game_id"], "seq_idx": {"$gt": seq["seq_idx"]}, "stable": True}, projection={"_id": False}).sort("seq_idx")

        for n in neighbours:
            return n
        
        return None
    

    def get_user_game_counts(self, game_id, user_id):   
        stats = self.mongo_client.thumbnails.user_game_stats.find_one({"game_id": game_id, "user_id": user_id}, projection={"_id": False})

        if not stats:
            return 0, {}, False
    
        return stats["total_points"], stats["total_per_digit_number"], True


    def get_user_game_stable_boundaries(self, game_id, user_id):
        stats = self.mongo_client.thumbnails.user_game_stats.find_one({"game_id": game_id, "user_id": user_id}, projection={"_id": False})

        if not stats:
            return [1000000, -1]
        
        return stats["stable_boundaries"]
    
    
    def save_user_game_counts(self, game_id, user_id, total_points, total_per_digit_number, stable_boundaries):
        to_insert = {"game_id": game_id, "user_id": user_id, "total_points": total_points, "total_per_digit_number": total_per_digit_number, "stable_boundaries": stable_boundaries}
        self.mongo_client.thumbnails.user_game_stats.find_one_and_replace({"game_id": game_id, "user_id": user_id}, to_insert, upsert=True)

    
    def get_last_sequence(self, game_id, user_id):
        last_sequence = [x for x in self.mongo_client.thumbnails.qoe_sequences.find({"game_id": game_id, "user_id": user_id, "start": {"$lte": self.since_date}}).sort("start", DESCENDING).limit(1)]

        if not last_sequence:
            return []
        
        return last_sequence[0]

    
    def get_last_sequence_latency(self, game_id, user_id):
        last_sequence = self.get_last_sequence(game_id, user_id)        
        init_idx = -1
    
        if last_sequence:
            start_dt = datetime.fromtimestamp(last_sequence["start"])

            to_return = set()
            for x in self.latency_db["latency_{}_{:02d}".format(start_dt.year, start_dt.month)].find({"game_id": game_id, "user_id": user_id, "date": {"$gte": last_sequence["start"]}}, projection={"_id": False}).sort("date"):
                if init_idx < 0 and "idx" in x:
                    init_idx = x["idx"]

                to_return.add(tuple([datetime.fromtimestamp(x["date"]), x["stream_id"]]))
            
            # Are there "next" collections to check?
            plus_one_month = start_dt + timedelta(days=31)
            while plus_one_month < datetime.now():
                for x in self.latency_db["latency_{}_{:02d}".format(plus_one_month.year, plus_one_month.month)].find({"game_id": game_id, "user_id": user_id, "date": {"$gte": last_sequence["start"]}}, projection={"_id": False}).sort("date"):               
                    if init_idx < 0 and "idx" in x:
                        init_idx = x["idx"]

                    to_return.add(tuple([datetime.fromtimestamp(x["date"]), x["stream_id"]]))

                plus_one_month += timedelta(days=31)

            return to_return, last_sequence, init_idx

        return [], None, 0
    

    def get_old_periods(self, game_id, user_id):
        was_stable = False
        sequences = []
        for idx, x in enumerate(self.mongo_client.thumbnails.qoe_sequences.find({"game_id": game_id, "user_id": user_id, "start": {"$lte": self.since_date}}).sort("start")):
            if idx != x["seq_idx"]:
                self.mongo_client.thumbnails.qoe_sequences.update_one({'_id':x["_id"]}, {"$set": {"seq_idx": idx}}, upsert=False)
                x["seq_idx"] = idx

            sequences.append(x)
            was_stable = was_stable or x["stable"]

        if not sequences:
            return [], False
        
        # Return without the last sequence
        return sorted(sequences[:-1], key=lambda x: x["seq_idx"]), was_stable            
    

