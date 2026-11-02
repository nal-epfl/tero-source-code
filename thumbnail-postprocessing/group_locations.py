from pymongo import MongoClient
from logger import get_logger

from utils import compare_locations
from config import mongo_host, mongo_port, mongo_user, mongo_password


class GroupLocations:
    def __init__(self) -> None:
        self.mongo_client = MongoClient("mongodb://{}:{}/".format(mongo_host, mongo_port), username=mongo_user, password=mongo_password)
        self.logger = get_logger("group_locations")


    def get_user_location(self, user_id):
        return self.mongo_client.location_data.users.find({"user_id": str(user_id)}, projection={"_id": False})


    def get_all_users(self):
        return [x["user_id"] for x in self.mongo_client.thumbnails.latency_presence.find()]


    def get_users_locations(self, users):
        several_locations = []
        users_location = {}

        for user_id in users:
            location = [x for x in self.get_user_location(user_id)]
            if len(location) == 1:           
                users_location[user_id] = location[0]["location"]
            elif len(location) > 1:
                several_locations.append(location)

        for l in several_locations:
            same_location = True
            to_store = l[0]
            
            for idx in range(1, len(l)):
                comparison_result = compare_locations(l[0]["location"], l[idx]["location"]) 

                same_location = same_location and comparison_result >= 0
                if comparison_result == 2:
                    to_store = l[idx]

            if same_location:
                users_location[l[0]["user_id"]] = to_store["location"]
            else:
                # Check if the different locations come from the same source
                source_ids = set([x.get("source_id", 0) for x in l])
                if len(source_ids) == 1:
                    # All locations come from the same source, take the latest one
                    sorted_by_version = sorted(l, key=lambda x: x.get("version_id", 0), reverse=True)
                    users_location[l[0]["user_id"]] = sorted_by_version[0]

        return users_location


    def store_locations(self, to_store):
        self.mongo_client.location_data.clean_users.drop()

        keys = list(to_store.keys())

        n_batches = 10
        batches_size = int(len(keys) / n_batches)

        keys_chunks = [keys[i:i + batches_size] for i in range(0, len(keys), batches_size)]

        for chunk in keys_chunks:
            to_insert_chunk = {}
            for k in chunk:
                to_insert_chunk[k] = to_store[k]

            self.mongo_client.location_data.clean_users.insert_one(to_insert_chunk)


    def run(self):
        self.logger.info("Getting all user locations...")
        users = self.get_all_users()
        user_locations = self.get_users_locations(users)

        self.logger.info("Finished fetching user locations")

        self.store_locations(user_locations)
        self.logger.info("Stored summarized user locations...")


if __name__ == "__main__":
    group_locations = GroupLocations()
    group_locations.run()