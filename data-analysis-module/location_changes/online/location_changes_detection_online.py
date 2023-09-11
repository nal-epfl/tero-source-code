import json
import multiprocessing
import redis

from datetime import datetime
from copy import deepcopy
from functools import partial
from pymongo import MongoClient, DESCENDING, ReturnDocument
from config import mongo_host, mongo_port, mongo_user, mongo_password, qoe_bands, redis_host, redis_password, redis_port
from utils.utils import group_sequences, get_stored_locations, get_biggest_division

from utils.logger import get_logger


chunk_size = 100
number_cores = 20
qoe_band = 15
factor = 2

class UserChangeDetection:
    def __init__(self):
        self.mongo_client = MongoClient("mongodb://{}:{}/".format(mongo_host, mongo_port), username=mongo_user, password=mongo_password)
        self.users_locations = get_stored_locations(self.mongo_client)


    def get_region_clusters(self, game_id, country, region):
        return [x for x in self.mongo_client.processed.clusters.find({"game_id": game_id, "country": country, "region": region})]


    def start_current_subseq(self, summary):
        latest_start = datetime(year=2021, month=5, day=1).timestamp() 
        
        for cluster in summary["changes"]["found_cluster"]:                
            if "current" in cluster["subsequence"] or latest_start < cluster["subsequence"]["dates"][-1][0]:
                latest_start = cluster["subsequence"]["dates"][-1][0]
                
        for cluster in summary["changes"]["not_found"]:                
            if "current" not in cluster or latest_start < cluster["dates"][-1][0]:
                latest_start = cluster["dates"][-1][0]

        return latest_start


    def separate_into_subsequences(self, game_id, user_id, changes, old_summary):
        dates = [x["date"] for x in self.mongo_client.processed.latency.find({"game_id": game_id, "user_id": user_id}, projection={"_id": False}).sort("date")]
        start_date = dates[0]
        end_date = dates[-1]
        
        if old_summary:
            start_date = self.start_current_subseq(old_summary)
                   
        subsequences = []
        transitions = []
        change = changes[0]

        for idx in range(0, len(changes)):        
            change = changes[idx]
            subsequences.append({"start": start_date, "end": change["date"]["start"], "min": change["groups"][0]["min"], "max": change["groups"][0]["max"], "streams": change["inside_stream"]})
            transitions.append({"idxs": [idx, idx+1], "possible_server_change": change["inside_stream"][0] == change["inside_stream"][1]})
            start_date = change["date"]["end"]
            
        subsequences.append({"start": change["date"]["end"], "end": end_date, "min": change["groups"][1]["min"], "max": change["groups"][1]["max"], "streams": change["inside_stream"]})

        return subsequences, transitions


    def unroll_cluster(self, subsequence):
        unrolled_version = []

        for limits in subsequence["dates"]:
            unrolled_version.append({"min": subsequence["min"], "max": subsequence["max"], "start": limits[0], "end": limits[1]})
        
        return unrolled_version
    

    def process_user_changes(self, game_id, user_id, changes):
        old_summary = self.mongo_client.processed.changes_summary.find_one({"game_id": game_id, "user_id": user_id})
        subsequences, transitions = self.separate_into_subsequences(game_id, user_id, changes, old_summary)
        
        old_groups = []
        last_assigned_group = 0

        if old_summary:
            old_subsequences = []
            for x in old_summary["changes"]["found_cluster"]:
                if "current" not in x["subsequence"]:
                    old_subsequences.extend([{**u, "old": True} for u in self.unroll_cluster(x["subsequence"])])

                old_groups.append({"min": x["subsequence"]["min"], "max": x["subsequence"]["max"], "group_idx": x["group_idx"]})
                last_assigned_group = max(last_assigned_group, x["group_idx"])

            for x in old_summary["changes"]["not_found"]:
                if "current" not in x:
                    old_subsequences.extend([{**u, "old": True} for u in self.unroll_cluster(x)])

            old_subsequences.extend(subsequences)
            old_subsequences.sort(key=lambda x: x["start"])

            grouped_subsequences = group_sequences(old_subsequences, qoe_band, factor)
        else:
            grouped_subsequences = group_sequences(subsequences, qoe_band, factor)

        flattened_subsequences = []

        for idx, group in enumerate(grouped_subsequences):
            for subseq in group["sequences"]:
                if "old" not in subseq:
                    found_old = False
                    for old in old_groups:
                        min_boundary = min(old["min"], subseq["min"])
                        max_boundary = max(old["max"], subseq["max"])

                        if max_boundary - min_boundary <= factor*qoe_band:
                            flattened_subsequences.append({"group_idx": old["group_idx"], **subseq, "min": min_boundary, "max": max_boundary})
                            found_old = True
                            break
                    
                    if not found_old:
                        flattened_subsequences.append({"group_idx": last_assigned_group+1+idx, **subseq})

        flattened_subsequences.sort(key=lambda x: x["start"])
        group_transitions = {}
                
        for idx in range(0, len(flattened_subsequences)-1):
            group_change = tuple(sorted([flattened_subsequences[idx]["group_idx"], flattened_subsequences[idx+1]["group_idx"]]))
            possible_server_change = transitions[idx]["possible_server_change"]

            if group_change not in group_transitions:
                group_transitions[group_change] = {"server_change": False, "streams": []}
            
            group_transitions[group_change]["server_change"] = group_transitions[group_change]["server_change"] or possible_server_change
            group_transitions[group_change]["streams"].append([list(set(flattened_subsequences[idx]["streams"])), [flattened_subsequences[idx]["start"], flattened_subsequences[idx]["end"]]])
            
        return flattened_subsequences, group_transitions, old_summary


    def get_possible_changes(self, user_id, game_id, since_date, factor=2):
        grouped_seqs = self.mongo_client.processed.grouped_sequences.find_one({"game_id": game_id, "user_id": user_id}, projection={"_id": False})
        groups = [{"group_idx": idx, **x} for idx, x in enumerate(grouped_seqs["grouped_sequences"])]

        qoe_with_group = []

        for s in self.mongo_client.processed.qoe.find({"game_id": game_id, "user_id": user_id}, projection={"_id": False}):
            if "is_spike" not in s and s["stable"]:
                for group in groups:
                    if group["min"] <= s["min"] and s["max"] <= group["max"]:
                        qoe_with_group.append({**s, "group": group["group_idx"]})
                        break
        
        if not qoe_with_group:
            return []

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

            if consecutive_sequences[idx-1]["end"] < since_date:
                continue
            
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


    def classify_changes(self, user_id, game_id, since_date, changes): 
        flattened_subsequences, group_transitions, old_summary = self.process_user_changes(game_id, user_id, changes)     
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
        groups_found = {}

        if old_summary:
            for not_found in old_summary["changes"]["not_found"]:
                dates = []
                for d in not_found["dates"]:
                    if d[1] < since_date: 
                        dates.append(d)

                not_found["dates"] = d
                changes_report["not_found"].append(not_found)

            for found in old_summary["changes"]["found_cluster"]:
                dates = []
                for d in found["subsequence"]["dates"]:
                    if d[1] < since_date: 
                        dates.append(d)

                found["subsequence"]["dates"] = dates
                groups_found[found["group_idx"]] = found

        groups_to_inspect = {}
        
        for idx in range(0, len(compiled_data["groups"])-1):
            group_change = tuple(sorted([compiled_data["groups"][idx]["group_idx"], compiled_data["groups"][idx+1]["group_idx"]]))

            if change_classifier[group_change]:
                if compiled_data["groups"][idx]["group_idx"] not in groups_to_inspect:
                    groups_to_inspect[compiled_data["groups"][idx]["group_idx"]] = {"min": compiled_data["groups"][idx]["min"], "max": compiled_data["groups"][idx]["max"], "dates": []}    

                groups_to_inspect[compiled_data["groups"][idx]["group_idx"]]["dates"].append([compiled_data["groups"][idx]["start"], compiled_data["groups"][idx]["end"]]) 

            if idx == len(compiled_data["groups"])-1:
                groups_to_inspect[compiled_data["groups"][idx]["group_idx"]]["current"] = True
                       
        if not groups_to_inspect:
            return

        for group_idx, group in groups_to_inspect.items():
            group_found_cluster = False

            if group_idx in groups_found:
                if "current" in group:
                    groups_found[group_idx]["subsequence"]["current"] = True

                min_boundary = min(groups_found[group_idx]["cluster"]["min"], group["min"])
                max_boundary = max(groups_found[group_idx]["cluster"]["max"], group["max"])

                groups_found[group_idx]["cluster"]["min"] = min_boundary
                groups_found[group_idx]["cluster"]["max"] = max_boundary

                groups_found[group_idx]["subsequence"]["dates"].extend(group["dates"])
                continue

            for cluster in clusters:
                min_boundary = min(cluster["min"], group["min"])
                max_boundary = max(cluster["max"], group["max"])

                # Careful here, you are hardcoding values
                if max_boundary - min_boundary <= qoe_band*factor:
                    group_found_cluster = True
                    cluster_copy = deepcopy(cluster)
                    if "_id" in cluster_copy:
                        cluster_copy.pop("_id")
                    
                    if "users" in cluster_copy:
                        cluster_copy.pop("users")

                    changes_report["found_cluster"].append({"cluster": cluster_copy, "subsequence": group, "group_idx": group_idx})
                    break
            
            if not group_found_cluster:
                changes_report["not_found"].append(group)

        # Merge the new changes report with those already found
        for group in groups_found.values():
            changes_report["found_cluster"].append(group)

        self.mongo_client.processed.changes_summary.find_one_and_replace({"game_id": game_id, "user_id": user_id}, {"game_id": game_id, "user_id": user_id, "location": location, "changes": changes_report})
        if self.mongo_client.processed.users_without_changes.count_documents({"game_id": game_id, "user_id": user_id}, limit=1) != 0:
            self.mongo_client.processed.users_without_changes.delete_one({"game_id": game_id, "user_id": user_id})

        return True       


def process_users_changes(users):
    change_detector = UserChangeDetection()
    users_without_changes = []
    users_with_changes = []

    for user in users:
        changes = change_detector.get_possible_changes(user["user_id"], user["game_id"], user["since"])
        if changes:
            changed = change_detector.classify_changes(user["user_id"], user["game_id"], user["since"], changes)
            if changed:
                users_with_changes.append(user)
        else:
            users_without_changes.append(user)

    return users, users_without_changes, users_with_changes



class LocationChangeDetection:
    def __init__(self, logger):
        self.logger = logger
        self.mongo_client = MongoClient("mongodb://{}:{}/".format(mongo_host, mongo_port), username=mongo_user, password=mongo_password)
        self.cache = redis.Redis(host=redis_host, port=redis_port, password=redis_password)


    def filter_users_games(self, users_to_study, threshold):
        user_games_with_changes = set()
        user_games_without = set()

        for user_game in users_to_study:
            sequences_end = datetime(year=2021, month=5, day=1).timestamp()
            
            for x in self.mongo_client.processed.grouped_sequences.find({"user_id": user_game["user_id"], "game_id": user_game["game_id"]}, projection={"_id": False}):
                totals = []
                total = 0
                for seq in x["grouped_sequences"]:
                    seq_total = []
                    
                    for s in seq["sequences"]:
                        #"is_spike" not in s and s["stable"] and
                        if user_game["since"] <= s["end"]:
                            seq_total.append(s["length"])
                            if sequences_end < s["end"]:
                                sequences_end = s["end"]
                    
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
                            user_games_with_changes.add(tuple([user_game["game_id"], user_game["user_id"], user_game["since"]]))
                        else:
                            user_games_without.add(tuple([user_game["game_id"], user_game["user_id"], user_game["since"]]))
                else:
                    user_games_without.add(tuple([user_game["game_id"], user_game["user_id"], user_game["since"]]))

        return user_games_with_changes, user_games_without


    def get_to_process(self):
        # log_entries = [json.loads(x.decode("utf8")) for x in self.cache.smembers("new_location_changes")]
        log_entries = [json.loads(x.decode("utf8")) for x in self.cache.spop("new_location_changes", count=self.cache.scard("new_location_changes"))]

        with open("new_location_changes.json", "w+") as f:
            for entry in log_entries:
                f.write("{}\n".format(json.dumps(entry)))

        entries_user_game = {}

        for entry in log_entries:
            if tuple([entry["user_id"], entry["game_id"]]) not in entries_user_game:
                entries_user_game[tuple([entry["user_id"], entry["game_id"]])] = []

            entries_user_game[tuple([entry["user_id"], entry["game_id"]])].append(entry["date"])

        return [{"user_id": x[0], "game_id": x[1], "since": min(k)} for x, k in entries_user_game.items()]


    def insert_no_change(self, users_without_changes):
        for u in users_without_changes:
            if self.mongo_client.processed.users_without_changes.count_documents({"game_id": u[0], "user_id": u[1]}, limit=1) == 0:
                if self.mongo_client.processed.changes_summary.count_documents({"game_id": u[0], "user_id": u[1]}, limit=1) == 0:
                    # New user
                    self.cache.sadd("to_distribution_no_change", json.dumps({"game_id": u[0], "user_id": u[1], "since": u[2]}))
                    self.mongo_client.processed.users_without_changes.insert_one({"game_id": u[0], "user_id": u[1]})
                else:
                    self.update_change_summary(u)
            else:
                # User already registered as not having changes, just add to distribution
                self.cache.sadd("to_distribution_no_change", json.dumps({"game_id": u[0], "user_id": u[1], "since": u[2]}))


    def update_change_summary(self, user):
        summary = self.mongo_client.processed.changes_summary.find_one({"game_id": user[0], "user_id": user[1]}, projection={"_id": False})

        if not summary:
            return
        
        # Users with previous changes should only be inserted if the sequence being extended has a corresponding cluster
        found_cluster = []
        for cluster in summary["changes"]["found_cluster"]:                
            if "current" not in cluster["subsequence"]:
                found_cluster.append({"cluster": cluster["cluster"], "subsequence": cluster["subsequence"]})
            else:
                modified_subsequence = deepcopy(cluster["subsequence"])
                modified_subsequence["dates"][1] = user[2]

                found_cluster.append({"cluster": cluster["cluster"], "subsequence": cluster["subsequence"]})
                self.cache.sadd("to_distribution_no_change", json.dumps({"game_id": user[0], "user_id": user[1], "since": user[2]}))

        summary["changes"]["found_cluster"] = found_cluster

        not_found_cluster = []
        for subsequence in summary["changes"]["not_found"]:                
            if "current" not in cluster:
                not_found_cluster.append(subsequence)
            else:
                modified_subsequence = deepcopy(subsequence)
                modified_subsequence["dates"][1] = user[2]

                not_found_cluster.append(subsequence)

        summary["changes"]["not_found"] = not_found_cluster

        self.mongo_client.processed.changes_summary.find_one_and_replace({"game_id": user[0], "user_id": user[1]}, summary)

        
    def run(self):
        to_process = self.get_to_process()
        self.logger.info("Found {} pairs to process".format(len(to_process)))      
        
        self.logger.info("Filtering users with possible changes")
        user_games_with, user_games_without = self.filter_users_games(to_process, threshold=1)
        self.insert_no_change(user_games_without)

        user_games_to_study = [{"game_id": u[0], "user_id": u[1], "since": u[2]} for u in user_games_with]

        self.logger.info("Users with possible changes: {}".format(len(user_games_to_study)))

        users_chunks = [user_games_to_study[i:i + chunk_size] for i in range(0, len(user_games_to_study), chunk_size)]
        pool = multiprocessing.Pool(number_cores)
        
        self.logger.info("Starting users with possible changes...")   

        tmp = partial(process_users_changes)
        results = pool.imap(func=tmp, iterable=users_chunks)

        users_with_changes = []
        users_without_changes = []
        ready = 0
        for r in results:
            ready += len(r[0])
            self.logger.info("Users finished: {}/{}".format(ready, len(user_games_to_study)))

            if r[1]:
                users_without_changes.extend(r[1])

            if r[2]:
                users_with_changes.extend(r[2])

        if users_without_changes:
            self.mongo_client.processed.users_without_changes.insert_many(users_without_changes)

        self.logger.info("Inserting users with changes. To insert: {}".format(len(users_with_changes)))
        if users_with_changes:
            for u in users_with_changes:
                self.cache.sadd("to_distribution_new_change", json.dumps(u))

        self.logger.info("Finished processing data...")
   

if __name__ == "__main__":
    logger = get_logger("location_change_analysis_online")
    
    location_changes = LocationChangeDetection(logger)
    location_changes.run()