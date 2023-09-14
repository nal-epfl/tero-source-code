import json
import redis

from pymongo import MongoClient, DESCENDING

from config import mongo_host, mongo_port, mongo_user, mongo_password, qoe_bands, redis_host, redis_port, redis_password
from utils.utils import get_stored_locations, get_biggest_division, group_sequences, get_users_by_region
from utils.logger import get_logger


class ClusterDetection:
    def __init__(self, logger):
        self.mongo_client = MongoClient("mongodb://{}:{}/".format(mongo_host, mongo_port), username=mongo_user, password=mongo_password)
        self.cache = redis.Redis(host=redis_host, port=redis_port, password=redis_password)
        self.logger = logger


    def summarize_sequences_by_user(self, users_data, threshold=0.8):
        summarize_sequences = []

        for user in users_data:
            grouped_sequences = self.mongo_client.processed.grouped_sequences.find_one({"game_id": user[0], "user_id": user[1]}, projection={"_id": False})
            if not grouped_sequences:
                continue

            totals = []
            total = 0

            for seq in grouped_sequences["grouped_sequences"]:
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

                summarize_sequences.append({"game_id": user[0], "user_id": user[1], "distance": distance, "to_cover": to_cover, "total": total})

        return summarize_sequences

    

    def check_if_in_cluster(self, user_seq, cluster):
        return cluster["min"] <= user_seq["min"] and user_seq["max"] <= cluster["max"]         


    def get_to_process(self):
        return [json.loads(x.decode("utf8")) for x in self.cache.spop("new_to_cluster", count=self.cache.scard("new_to_cluster"))]


    def compute_clusters(self, qoe_band=15, threshold=0.8, factor=2):
        to_process = self.get_to_process()

        self.logger.info("Getting all users locations...")
        users_locations = get_stored_locations(self.mongo_client)

        sequences_per_user = self.summarize_sequences_by_user(to_process, threshold)
        
        changed_clusters = []
                
        for x in sequences_per_user:
            location = users_locations.get(x["user_id"], {})

            if not location:
                continue

            division, _ = get_biggest_division(location)

            if not division:
                continue
                       
            containing_cluster = None

            # Find the cluster where this user is currently contained: start from the clusters that have already changed
            for changed in changed_clusters:
                if changed["cluster"]["game_id"] == x["game_id"] and changed["cluster"]["country"] == location["country_code"] and changed["cluster"]["region"] == division:
                    if x["user_id"] in set(changed["cluster"]["users"]):
                        containing_cluster = cluster
                        break

            if not containing_cluster:
                for cluster in self.mongo_client.processed.clusters.find({"game_id": x["game_id"], "country": location["country_code"], "region": division}, projection={"_id": False}):
                    if x["user_id"] in set(cluster["users"]):
                        containing_cluster = cluster
                        break
        
            # User should be clustered
            if (len(x["to_cover"]) == 1 or len([dist for dist in x["distance"] if dist > qoe_bands.get(x["game_id"], 15)]) == 0):
                user_grouped = group_sequences(x["to_cover"], qoe_band, factor=factor)
                if len(user_grouped) == 1:
                    if not containing_cluster:
                        # Case 1: This user should be clustered but currently it is not 
                        changed_clusters.append({"cluster": {"game_id": x["game_id"], "country": location["country_code"], "region": division, "users": set([x["user_id"]])}, "user": x["user_id"]})                 
                    elif not self.check_if_in_cluster(user_grouped[0], containing_cluster):
                        # This user is clustered but it is in the wrong cluster
                        changed_clusters.append({"cluster": containing_cluster, "user": x["user_id"]})
                elif containing_cluster:
                    # Case 2: I wasn't able to group the user's sequences, remove them from the cluster
                    changed_clusters.append({"cluster": containing_cluster, "user": x["user_id"]})
              
            else:
                # Case 2: The user shouldn't be included in the cluster and it is, remove it.
                if containing_cluster:
                    changed_clusters.append({"cluster": containing_cluster, "user": x["user_id"]})

        # Group the changed clusters
        grouped_clusters = {}
        
        for c in changed_clusters:
            if c["cluster"]["game_id"] not in grouped_clusters:
                grouped_clusters[c["cluster"]["game_id"]] = {}
            if c["cluster"]["country"] not in grouped_clusters[c["cluster"]["game_id"]]:
                grouped_clusters[c["cluster"]["game_id"]][c["cluster"]["country"]] = {}
            if c["cluster"]["region"] not in grouped_clusters[c["cluster"]["game_id"]][c["cluster"]["country"]]:
                grouped_clusters[c["cluster"]["game_id"]][c["cluster"]["country"]][c["cluster"]["region"]] = set()

            grouped_clusters[c["cluster"]["game_id"]][c["cluster"]["country"]][c["cluster"]["region"]].add(c["user"])
            
        self.logger.info("Recomputing modified clusters.")        

        if grouped_clusters:
            for game_id, clusters_per_country in grouped_clusters.items():
                for country, divisions in clusters_per_country.items():
                    for division, new_users in divisions.items():
                        users = new_users
                        for cluster in self.mongo_client.processed.clusters.find({"game_id": game_id, "country": country, "region": division}, projection={"_id": False}):
                            users.update(cluster["users"])    
                        
                        per_user = []
                        
                        sequences = self.summarize_sequences_by_user([[game_id, user] for user in users], threshold)
                        for s in sequences:
                            user_grouped = group_sequences(s["to_cover"], qoe_band, factor=factor)
                            if len(user_grouped) == 1:
                                per_user.append({"user_id": s["user_id"], **user_grouped[0]})

                    grouped = group_sequences(per_user, qoe_band, factor=factor)
                    if grouped:
                        sequences = sorted(grouped, key=lambda x: len(x["sequences"]), reverse=True)
                        # Remove all old clusters and then insert
                        self.mongo_client.processed.clusters.delete_many({"game_id": game_id, "country": country, "region": division})
                        self.mongo_client.processed.clusters.insert_many([{"game_id": game_id, "country": country, "region": division, "min": x["min"], "max": x["max"], "coverage": 100*len(x["sequences"])/len(per_user), 
                                                                           "n_users": len(x["sequences"]), "users": [s["user_id"] for s in x["sequences"]]} for x in sequences])

