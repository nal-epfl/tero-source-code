import multiprocessing

from copy import deepcopy
from functools import partial
from pymongo import MongoClient, DESCENDING, ReturnDocument
from config import mongo_host, mongo_port, mongo_user, mongo_password, qoe_bands
from utils.utils import group_sequences, get_stored_locations, get_biggest_division

from utils.logger import get_logger


# You need to work on making this whole process incremental

chunk_size = 1000
number_cores = 20


class UserChangeDetection:
    def __init__(self):
        self.mongo_client = MongoClient("mongodb://{}:{}/".format(mongo_host, mongo_port), username=mongo_user, password=mongo_password)
        self.users_locations = get_stored_locations(self.mongo_client)


    def get_region_clusters(self, game_id, country, region):
        return [x for x in self.mongo_client.processed.clusters.find({"game_id": game_id, "country": country, "region": region})]


    def separate_into_subsequences(self, game_id, user_id, changes):
        dates = [x["date"] for x in self.mongo_client.processed.latency.find({"game_id": game_id, "user_id": user_id}, projection={"_id": False}).sort("date")]
        
        start_date = dates[0]
        end_date = dates[-1]

        subsequences = []
        transitions = []
        change = changes[0]

        for idx in range(0, len(changes)):        
            change = changes[idx]
            subsequences.append({"start": start_date, "end": change["date"]["start"], "min": change["groups"][0]["min"], "max": change["groups"][0]["max"], "subseq_idx": idx, "streams": change["inside_stream"]})
            transitions.append({"idxs": [idx, idx+1], "possible_server_change": change["inside_stream"][0] == change["inside_stream"][1]})
            start_date = change["date"]["end"]
            
        subsequences.append({"start": change["date"]["end"], "end": end_date, "min": change["groups"][1]["min"], "max": change["groups"][1]["max"], "subseq_idx": len(changes), "streams": change["inside_stream"]})

        return subsequences, transitions


    def process_user_changes(self, game_id, user_id, changes):
        subsequences, transitions = self.separate_into_subsequences(game_id, user_id, changes)

        grouped_subsequences = group_sequences(subsequences, qoe_band=15, factor=2)
        flattened_subsequences = []
        for idx, group in enumerate(grouped_subsequences):
            for subseq in group["sequences"]:
                flattened_subsequences.append({"group_idx": idx, **subseq})

        flattened_subsequences.sort(key=lambda x: x["subseq_idx"])
        group_transitions = {}
        for idx in range(0, len(flattened_subsequences)-1):
            group_change = tuple(sorted([flattened_subsequences[idx]["group_idx"], flattened_subsequences[idx+1]["group_idx"]]))
            possible_server_change = transitions[idx]["possible_server_change"]

            if group_change not in group_transitions:
                group_transitions[group_change] = {"server_change": False, "streams": []}
            
            group_transitions[group_change]["server_change"] = group_transitions[group_change]["server_change"] or possible_server_change
            group_transitions[group_change]["streams"].append([list(set(flattened_subsequences[idx]["streams"])), [flattened_subsequences[idx]["start"], flattened_subsequences[idx]["end"]]])

        return flattened_subsequences, group_transitions


    def get_possible_changes(self, user_id, game_id, factor=2):
        grouped_seqs = self.mongo_client.processed.grouped_sequences.find_one({"game_id": game_id, "user_id": user_id}, projection={"_id": False})
        groups = [{"group_idx": idx, **x} for idx, x in enumerate(grouped_seqs["grouped_sequences"])]

        qoe_with_group = []

        for s in self.mongo_client.processed.qoe.find({"game_id": game_id, "user_id": user_id}, projection={"_id": False}):
            if "is_spike" not in s and s["stable"]:
                for group in groups:
                    if group["min"] <= s["min"] and s["max"] <= group["max"]:
                        qoe_with_group.append({**s, "group": group["group_idx"]})
                        break
        
        consecutive_sequences = []
        current_sequence = [qoe_with_group[0]]

        for qoe in qoe_with_group[1:]:
            if current_sequence[0]["group"] == qoe["group"]:
                current_sequence.append(qoe)
            else:
                consecutive_sequences.append({"group": current_sequence[0]["group"], "start": current_sequence[0]["start"], "end": current_sequence[-1]["end"]})
                current_sequence = [qoe]

        if current_sequence:
            consecutive_sequences.append({"group": current_sequence[0]["group"], "start": current_sequence[0]["start"], "end": current_sequence[-1]["end"]})

        transitions = []

        for idx in range(1, len(consecutive_sequences)):
            start_group = groups[consecutive_sequences[idx-1]["group"]]
            end_group = groups[consecutive_sequences[idx]["group"]]
            
            start_stream = self.mongo_client.processed.latency.find_one({"game_id": game_id, "user_id": user_id, "date": consecutive_sequences[idx-1]["end"]}, projection={"_id": False})
            end_stream = self.mongo_client.processed.latency.find_one({"game_id": game_id, "user_id": user_id, "date": consecutive_sequences[idx]["start"]}, projection={"_id": False})

            transitions.append({"game_id": game_id, "user_id": user_id, "transition": end_group["min"] - start_group["min"], "date": {"start": consecutive_sequences[idx-1]["end"], "end": consecutive_sequences[idx]["start"]}, 
                                "inside_stream": (start_stream["stream_id"] if start_stream else "", end_stream["stream_id"] if end_stream else ""), 
                                                    "groups": [{"group_idx": start_group["group_idx"], "min": start_group["min"], "max": start_group["max"]}, 
                                                            {"group_idx": end_group["group_idx"], "min": end_group["min"], "max": end_group["max"]}]})
        
        possible_changes = [t for t in transitions if abs(t["transition"]) > factor*qoe_bands.get(game_id, 15)]
        if possible_changes:
            self.mongo_client.processed.possible_changes.insert_many(possible_changes)

        return possible_changes


    # I honestly think I'm being too harsh here directly discarding users without region data: you could work with countrt clusters
    def classify_changes(self, user_id, game_id, changes): 
        flattened_subsequences, group_transitions = self.process_user_changes(game_id, user_id, changes)     
        for x in flattened_subsequences:
            x.pop("streams")       

        compiled_data = {"groups": flattened_subsequences, "changes": [{"groups": list(k), "change": v} for k, v in group_transitions.items()]}

        location = self.users_locations.get(user_id)
        if not location:
            return

        division, _ = get_biggest_division(location)

        if not division:
            return

        clusters = self.get_region_clusters(game_id, location["country_code"], division)
        if not clusters:
            return

        change_classifier = {}
        for l in compiled_data["changes"]:
            change_classifier[tuple(l["groups"])] = l["change"]

        changes_report = {"found_cluster": [], "not_found": []}

        groups_to_inspect = {}
        
        for idx in range(0, len(compiled_data["groups"])-1):
            group_change = tuple(sorted([compiled_data["groups"][idx]["group_idx"], compiled_data["groups"][idx+1]["group_idx"]]))

            if change_classifier[group_change]:
                if compiled_data["groups"][idx]["group_idx"] not in groups_to_inspect:
                    groups_to_inspect[compiled_data["groups"][idx]["group_idx"]] = {"min": compiled_data["groups"][idx]["min"], "max": compiled_data["groups"][idx]["max"], "dates": []}    

                groups_to_inspect[compiled_data["groups"][idx]["group_idx"]]["dates"].append([compiled_data["groups"][idx]["start"], compiled_data["groups"][idx]["end"]]) 

            if idx == len(compiled_data["groups"])-2:
                groups_to_inspect[compiled_data["groups"][idx]["group_idx"]]["current"] = True

        if not groups_to_inspect:
            return

        for group_idx, group in groups_to_inspect.items():
            group_found_cluster = False
            for cluster in clusters:
                min_boundary = min(cluster["min"], group["min"])
                max_boundary = max(cluster["max"], group["max"])

                # Careful here, you are hardcoding values
                if max_boundary - min_boundary <= 15*2:
                    group_found_cluster = True
                    cluster_copy = deepcopy(cluster)
                    if "_id" in cluster_copy:
                        cluster_copy.pop("_id")
                    
                    if "users" in cluster_copy:
                        cluster_copy.pop("users")

                    changes_report["found_cluster"].append({"cluster": cluster, "subsequence": group, "group_idx": group_idx})
                    break
            
            if not group_found_cluster:
                changes_report["not_found"].append(group)

        self.mongo_client.processed.changes_summary.insert_one({"game_id": game_id, "user_id": user_id, "location": location, "changes": changes_report})
       


def process_users_changes(users):
    change_detector = UserChangeDetection()
    users_without_changes = []

    for user in users:
        changes = change_detector.get_possible_changes(user["user_id"], user["game_id"])
        if changes:
            change_detector.classify_changes(user["user_id"], user["game_id"], changes)
        else:
            users_without_changes.append(user)

    return users, users_without_changes



class LocationChangeDetection:
    def __init__(self, logger):
        self.logger = logger
        self.mongo_client = MongoClient("mongodb://{}:{}/".format(mongo_host, mongo_port), username=mongo_user, password=mongo_password)


    def filter_users_games(self, threshold):
        user_games_with_changes = set()
        user_games_without = set()

        for x in self.mongo_client.processed.grouped_sequences.find(projection={"_id": False}):
            totals = []
            total = 0
            for seq in x["grouped_sequences"]:
                seq_total = [s["length"] for s in seq["sequences"] if "is_spike" not in s and s["stable"]]
                if seq_total:
                    totals.append({"total": sum(seq_total), "min": seq["min"], "max": seq["max"]})
                    total += sum(seq_total)

            if total > 0:
                coverage = sorted([{**t, "coverage": t["total"]/total} for t in totals], key=lambda x: x["coverage"], reverse=True)
                
                total_coverage = 0
                to_cover = []
                distance = []
                
                for idx, c in enumerate(coverage):
                    total_coverage += c["coverage"]
                    to_cover.append(c)

                    if idx > 0:
                        distance.append(abs(c["min"] - coverage[0]["min"]))

                    if total_coverage >= threshold:
                        break

                    if len(to_cover) > 1 and len([dist for dist in distance if dist > qoe_bands.get(x["game_id"], 15)]) > 0:
                        user_games_with_changes.add(tuple([x["game_id"], x["user_id"]]))
                    else:
                        user_games_without.add(tuple([x["game_id"], x["user_id"]]))
            else:
                user_games_without.add(tuple([x["game_id"], x["user_id"]]))

        return user_games_with_changes, user_games_without


    def clean_old_values(self):
        self.mongo_client.processed.possible_changes.drop()
        self.mongo_client.processed.users_without_changes.drop()
        self.mongo_client.processed.changes_summary.drop()


    def create_indexes(self):
        self.mongo_client.processed.users_without_changes.create_index("game_id")
        self.mongo_client.processed.users_without_changes.create_index("user_id")

        self.mongo_client.processed.possible_changes.create_index("game_id")
        self.mongo_client.processed.possible_changes.create_index("user_id")
        self.mongo_client.processed.possible_changes.create_index("date.start")
        self.mongo_client.processed.possible_changes.create_index("date.end")

        self.mongo_client.processed.changes_summary.create_index("game_id")
        self.mongo_client.processed.changes_summary.create_index("user_id")



    def run(self):      
        self.logger.info("Cleaning old stored values...")
        self.clean_old_values()

        self.logger.info("Filtering users with possible changes")
        user_games_with, user_games_without = self.filter_users_games(threshold=1)
        
        user_games_without_changes = [{"game_id": u[0], "user_id": u[1]} for u in user_games_without]

        if user_games_without_changes:
            self.mongo_client.processed.users_without_changes.insert_many(user_games_without_changes)

        user_games_to_study = [{"game_id": u[0], "user_id": u[1]} for u in user_games_with]

        self.logger.info("Users with possible changes: {}".format(len(user_games_to_study)))

        users_chunks = [user_games_to_study[i:i + chunk_size] for i in range(0, len(user_games_to_study), chunk_size)]
        pool = multiprocessing.Pool(number_cores)
        
        self.logger.info("Starting users with possible changes...") 

        tmp = partial(process_users_changes)
        results = pool.imap(func=tmp, iterable=users_chunks)

        users_without_changes = []
        ready = 0
        for r in results:
            ready += len(r[0])
            self.logger.info("Users finished: {}/{}".format(ready, len(user_games_to_study)))

            if r[1]:
                users_without_changes.extend(r[1])

        if users_without_changes:
            self.mongo_client.processed.users_without_changes.insert_many(users_without_changes)

        self.logger.info("Indexing data...")
        self.create_indexes()
        self.logger.info("Finished processing data...")
   

if __name__ == "__main__":
   logger = get_logger("location_changes_detection")

   location_changes = LocationChangeDetection(logger)
   location_changes.run()
