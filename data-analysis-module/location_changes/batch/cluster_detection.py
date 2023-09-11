from pymongo import MongoClient, DESCENDING
from config import mongo_host, mongo_port, mongo_user, mongo_password, qoe_bands
from utils.utils import get_stored_locations, get_biggest_division, group_sequences
from utils.logger import get_logger


class ClusterDetection:
    def __init__(self, logger):
        self.mongo_client = MongoClient("mongodb://{}:{}/".format(mongo_host, mongo_port), username=mongo_user, password=mongo_password)
        self.logger = logger


    def summarize_sequences_by_user(self, threshold=0.8):
        summarize_sequences = []

        self.logger.info("Summarizing user sequences")

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

                summarize_sequences.append({"game_id": x["game_id"], "user_id": x["user_id"], "distance": distance, "to_cover": to_cover, "total": total})

        return summarize_sequences


    def compute_clusters(self, qoe_band=15, threshold=0.8, factor=2):
        self.logger.info("Getting all users locations...")
        users_locations = get_stored_locations(self.mongo_client)

        sequences_per_user = self.summarize_sequences_by_user(threshold)
        
        sequences_per_game_country = {}
                
        for x in [y for y in sequences_per_user if (len(y["to_cover"]) == 1 or len([dist for dist in y["distance"] if dist > qoe_bands.get(y["game_id"], 15)]) == 0)]:
            location = users_locations.get(x["user_id"], {})

            if not location:
                continue

            division, _ = get_biggest_division(location)

            if not division:
                continue

            if x["game_id"] not in sequences_per_game_country:
                sequences_per_game_country[x["game_id"]] = {}

            if location["country_code"] not in sequences_per_game_country[x["game_id"]]:
                sequences_per_game_country[x["game_id"]][location["country_code"]] = {}

            if division not in sequences_per_game_country[x["game_id"]][location["country_code"]]:
                sequences_per_game_country[x["game_id"]][location["country_code"]][division] = []

            sequences_per_game_country[x["game_id"]][location["country_code"]][division].append(x)

        self.logger.info("Storing the clusters")        

        for game_id, sequences_per_country in sequences_per_game_country.items():
            for country, sequences_per_division in sequences_per_country.items():
                for division, sequences in sequences_per_division.items():
                    per_user = []
                    for s in sequences:
                        user_grouped = group_sequences(s["to_cover"], qoe_band, factor=factor)
                        if len(user_grouped) == 1:
                            per_user.append({"user_id": s["user_id"], **user_grouped[0]})
                            
                    grouped = group_sequences(per_user, qoe_band, factor=factor)
                    if grouped:
                        sequences = sorted(grouped, key=lambda x: len(x["sequences"]), reverse=True)
                        self.mongo_client.processed.clusters.insert_many([{"game_id": game_id, "country": country, "region": division, "min": x["min"], "max": x["max"], "coverage": 100*len(x["sequences"])/len(per_user), 
                                                                           "n_users": len(x["sequences"]), "users": [s["user_id"] for s in x["sequences"]]} for x in sequences])


    def clean_old_clusters(self):
        self.logger.info("Cleaning previous clusters...")
        self.mongo_client.processed.clusters.drop()


    def index_clusters(self):
        self.logger.info("Indexing...")
        
        self.mongo_client.processed.clusters.create_index("country")
        self.mongo_client.processed.clusters.create_index("region")
        self.mongo_client.processed.clusters.create_index("game_id")
        
        self.logger.info("Finished processing")        


if __name__ == "__main__":
    logger = get_logger("cluster_detection")

    cluster_detector = ClusterDetection(logger)
    cluster_detector.clean_old_clusters()
    cluster_detector.compute_clusters()
    cluster_detector.index_clusters()
    