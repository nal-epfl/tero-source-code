import uuid
from pymongo import MongoClient
from config import  mongo_host, mongo_password, mongo_user
from logger import get_logger


qoe_band=20
factor=2


class BandsAnalyzer:
    def __init__(self):
        self.logger = get_logger("cluster-bands")
        self.mongo_client = MongoClient("mongodb://{}:27017/".format(mongo_host),
                                    username=mongo_user,
                                    password=mongo_password)
        self.users_locations = {}


    def init(self):
        self.logger.info("Starting process...")
        self.logger.info("Cleaning old clusters")
        self.mongo_client.clustering.clusters.drop()

        self.users_locations = self.get_stored_locations()
    
    
    def get_stored_locations(self):
        users_locations = {}

        for x in self.mongo_client.location_data.clean_users.find({}, projection={"_id": False}):
            users_locations = {**users_locations, **x}

        return users_locations


    @staticmethod
    def compute_clusters(sequences):    
        first_seq = None
        for x in sequences:
            first_seq = x
            break
        
        if not first_seq:
            return []

        grouped = [{"min": first_seq["min"], "max": first_seq["max"], "sequences": [first_seq]}]

        # Step 1: put together all bands that are "equivalent"
        for s in sequences[1:]:
            found = False

            for group in grouped:
                if group["min"] <= s["min"] and s["max"] <= group["max"]:
                    group["sequences"].append(s)
                    found = True
                    break
            
            if not found:
                grouped.append({"min": s["min"], "max": s["max"], "sequences": [s]})

        # Step 2: Find groups of bands that form a cluster. Give priority to the band groups with the highest coveage
        grouped.sort(key=lambda x: len(x["sequences"]), reverse=True)

        grouped_sequences = [{"min": grouped[0]["min"], "max": grouped[0]["max"], "sequences": grouped[0]["sequences"]}]
        total_bands = len(grouped[0]["sequences"])


        for g in grouped[1:]:
            found = False

            for group in grouped_sequences:
                min_boundary = min(g["min"], group["min"])
                max_boundary = max(g["max"], group["max"])

                if max_boundary - min_boundary <= factor*qoe_band:
                    group["min"] = min_boundary
                    group["max"] = max_boundary
                    group["sequences"].extend(g["sequences"])
                    total_bands += len(g["sequences"])
                    found = True
                    break
            
            if not found:
                grouped_sequences.append(g)
                total_bands += len(g["sequences"])

        to_return = []
        # Count all users
        for cluster in grouped_sequences:
            cluster_users = set()
            for s in cluster["sequences"]:
                cluster_users.add(s["user_id"])

            to_return.append({"cluster_id": str(uuid.uuid1()), "min": cluster["min"], "max": cluster["max"], "coverage": 100*len(cluster["sequences"])/total_bands, "users": len(cluster_users)})

        return to_return


    def clusters_per_country(self, good_per_user):
        good_per_country_and_game = {}
        for user_id, streams in good_per_user.items():
            user_location = self.users_locations.get(user_id)

            if user_location["country_code"] not in good_per_country_and_game:
                good_per_country_and_game[user_location["country_code"]] = {}
            
            for stream in streams:
                if stream["game_id"] not in good_per_country_and_game[user_location["country_code"]]:
                    good_per_country_and_game[user_location["country_code"]][stream["game_id"]] = []

                good_per_country_and_game[user_location["country_code"]][stream["game_id"]].append(stream)


        for country, streams_per_game in good_per_country_and_game.items():
            for game_id, streams in streams_per_game.items():
                all_bands = []
                for stream in streams:
                    all_bands.extend([{"user_id": stream["user_id"], **b} for b in stream["bands"]])

                clusters = self.compute_clusters(all_bands)
                self.mongo_client.clustering.clusters.insert_one({"game_id": game_id, "country": country, "clusters": clusters})


    def clusters_per_region(self, good_per_user):
        region_to_country = {}
        
        good_per_region_and_game = {}
        for user_id, streams in good_per_user.items():
            user_location = self.users_locations.get(user_id)

            if not "region" in user_location:
                continue

            if user_location["region"] not in good_per_region_and_game:
                good_per_region_and_game[user_location["region"]] = {}
            
            region_to_country[user_location["region"]] = user_location["country_code"]

            for stream in streams:
                if stream["game_id"] not in good_per_region_and_game[user_location["region"]]:
                    good_per_region_and_game[user_location["region"]][stream["game_id"]] = []

                good_per_region_and_game[user_location["region"]][stream["game_id"]].append(stream)

        for region, streams_per_game in good_per_region_and_game.items():
            for game_id, streams in streams_per_game.items():
                all_bands = []
                for stream in streams:
                    all_bands.extend([{"user_id": stream["user_id"], **b} for b in stream["bands"]])

                clusters = self.compute_clusters(all_bands)
                self.mongo_client.clustering.clusters.insert_one({"game_id": game_id, "country": region_to_country[region], "region": region, "clusters": clusters})


    def clusters_per_city(self, good_per_user):
        city_to_country = {}

        good_per_city_and_game = {}
        for user_id, streams in good_per_user.items():
            user_location = self.users_locations.get(user_id)

            if not "city" in user_location:
                continue

            if user_location["city"] not in good_per_city_and_game:
                good_per_city_and_game[user_location["city"]] = {}
            
            city_to_country[user_location["city"]] = user_location["country_code"]

            for stream in streams:
                if stream["game_id"] not in good_per_city_and_game[user_location["city"]]:
                    good_per_city_and_game[user_location["city"]][stream["game_id"]] = []

                good_per_city_and_game[user_location["city"]][stream["game_id"]].append(stream)


        for city, streams_per_game in good_per_city_and_game.items():
            for game_id, streams in streams_per_game.items():
                all_bands = []
                for stream in streams:
                    all_bands.extend([{"user_id": stream["user_id"], **b} for b in stream["bands"]])

                clusters = self.compute_clusters(all_bands)
                self.mongo_client.clustering.clusters.insert_one({"game_id": game_id, "country": city_to_country[city], "city": city, "clusters": clusters, "n_streams": len(streams)})


    def run(self):
        self.logger.info("Grouping streams per user")
        
        good_per_user = {}
        for good_stream in self.mongo_client.clustering.good_streams.find(projection={"_id": False}):
            # For now, let's skip streams from users without location
            user_location = self.users_locations.get(good_stream["user_id"])
            if not user_location or "country_code" not in user_location:
                continue

            if good_stream["user_id"] not in good_per_user:
                good_per_user[good_stream["user_id"]] = []

            good_per_user[good_stream["user_id"]].append(good_stream)
        
        self.logger.info("Found data for {} users".format(len(good_per_user)))

        # Pending questions about the clusters
        # Per game? It makes sense because the servers are in different locations.
        # It should be normalized by distance?
        # What is comparable is the width of the bands

       # Now, clusters need to be computed per country / region / city

        # Per country
        self.clusters_per_country(good_per_user)

        # Per region
        self.clusters_per_region(good_per_user)

        # Per city
        self.clusters_per_city(good_per_user)

        self.logger.info("Finished processing")


if __name__ == "__main__":
    analyzer = BandsAnalyzer()
    analyzer.init()
    analyzer.run()
