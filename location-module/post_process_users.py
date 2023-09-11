import json
import redis
import sys

from datetime import datetime
from logger import get_logger
from pymongo import MongoClient

from config import redis_host, redis_port, redis_password, storage, base_path, mongo_host, mongo_port, mongo_user, mongo_password
from search_location import LocationParser
from db.online_storage import OnlineStorage


def compare_locations(loc1, loc2):
    if loc1.get("lat", -1) == loc2.get("lat", 1) and loc1.get("long", -1) == loc2.get("long", 1):
        return 0

    for key, value in loc1.items():
        #Note: country_code is used instead of country, that's why it's safe to skip it altogether
        if key in ["country","lat", "long"]:
            continue

        if key not in loc2:
            return 1
        elif loc2[key] != value:            
            return -1

    if len(loc2.keys()) > len(loc1.keys()):
        return 2

    return 0


def are_dicts_equal(dict1, dict2):
    keys = []
    if "external_id" in dict1:
        keys = ["external_id", "location"]
    else:
        keys = ["location"]

    for key in keys:
        if key not in dict2 or dict2[key] != dict1[key]:
            return False

    return True



def decide_conflict(storage, new_entry, old_entry):
    result = compare_locations(old_entry["location"], new_entry["location"])
    if result < 0:
        # New location!
        storage.insert_new_user({**new_entry, "version_id": old_entry.get("version_id", 0) + 1})
        logger.info("New location found. Inserting: {}".format({**new_entry, "version_id": old_entry.get("version_id", 0) + 1}))
    elif result == 2:
        # Same location, but more specific
        storage.delete_user(old_entry)
        storage.insert_new_user({**new_entry, "version_id": old_entry.get("version_id", 0)})
        logger.info("More specific location found. Inserting: {}".format({**new_entry, "version_id": old_entry.get("version_id", 0)}))  


if __name__ == "__main__":
    location_parser = LocationParser()
    storage_controller = OnlineStorage()

    logger = get_logger("post_process_users")
    backup_storage = "{}/parsed_users".format(storage)
    
    cache = redis.Redis(host=redis_host, port=redis_port, db=0, password=redis_password)
    mongo_client = MongoClient("mongodb://{}:{}/".format(mongo_host, mongo_port), username=mongo_user, password=mongo_password)

    to_postprocess = cache.spop("parsed_users", count=cache.scard("parsed_users"))
    if not to_postprocess:
        logger.info("Nothing to process. Finishing the process.")
        sys.exit(0)

    logger.info("Found {} users to post-process.".format(len(to_postprocess)))

    scheduled_to_be_inserted = {}
    repeated_insertion = {}

    with open("{}/{}.json".format(base_path, datetime.now().strftime('%Y-%m-%d-%H-%M-%S')), "w+") as f:
        for tp in to_postprocess:
            try:
                data = json.loads(tp.strip().decode("utf-8"))
            except Exception as e:
                continue 
            
            f.write(json.dumps(data) + "\n")
            user_id = str(data['user_id'])

            if "url" in data:
                data["user_id"] = data["url"]
                data.pop("url")

            if user_id not in repeated_insertion:
                if user_id not in scheduled_to_be_inserted:
                    scheduled_to_be_inserted[user_id] = data
                else:
                    if not are_dicts_equal(scheduled_to_be_inserted[user_id], data):
                        repeated_insertion[user_id] = [scheduled_to_be_inserted[user_id], data]
            else:
                found_equal = False

                for d in repeated_insertion[user_id]:
                    found_equal = found_equal or are_dicts_equal(d, data)

                if not found_equal:
                    repeated_insertion[user_id].append(data)

    conflicts_to_check = {}
    logger.info("Finished checking the users. Scheduled to be inserted: {}; repeated entries: {}".format(len(scheduled_to_be_inserted.keys()), len(repeated_insertion.keys())))

    for user_id, data in scheduled_to_be_inserted.items():
        if user_id in repeated_insertion:
            continue

        existing_entries = [x for x in mongo_client.location.users.find({"user_id": user_id})]
        
        if not existing_entries:
            # Careful: the user could be in the conflicts pile instead
            if mongo_client.location.conflicts.count_documents({"user_id": user_id}, limit=1) == 0:
                storage_controller.insert_new_user(data)
        else:
            if mongo_client.location.users.count_documents({"user_id": user_id, "location": data["location"]}, limit=1) == 0:
                if user_id not in conflicts_to_check:
                    conflicts_to_check[user_id] = {"existing_entries": existing_entries, "fresh_data": []}

                conflicts_to_check[user_id]["fresh_data"].append(data)


    for user_id, repeated in repeated_insertion.items():
        existing_entries = [x for x in mongo_client.location.users.find({"user_id": user_id})]
        merged = location_parser.merge_georesults([x["location"] for x in repeated])

        if not existing_entries:   
            if mongo_client.location.conflicts.count_documents({"user_id": user_id}, limit=1) == 0:     
                if merged:
                    # The locations of the repeated data are consistent, so they can all be inserted. 
                    # Note: even if the repeated elements come from the same source, no ordering is possible in this case (I consider that all of them came from the same batch).                    
                    for x in repeated:
                        storage_controller.insert_new_user(x)
                else:
                    # There's a location conflict: you can not insert these users.
                    for x in repeated:
                        storage_controller.insert_new_user(x)
                    
        else:
            if user_id not in conflicts_to_check:
                conflicts_to_check[user_id] = {"existing_entries": existing_entries, "fresh_data": []}

            conflicts_to_check[user_id]["fresh_data"].extend(repeated)

    logger.info("Finished inserting users. Conflicts detected: {}".format(len(conflicts_to_check.keys())))

    for twitch_id, conflict in conflicts_to_check.items():
        new_sources = []

        for c in conflict["fresh_data"]:                        
            if "external_id" not in c:
                source_matches = [x for x in conflict["existing_entries"] if "external_id" not in x]
                if source_matches:
                    if len(source_matches) == 1:
                        decide_conflict(storage_controller, c, source_matches[0])
                    else:
                        # If I have more than one location from these source, I need to sort them by index (least-recent to more-recent)
                        sorted_source_matches = sorted(source_matches, key=lambda x: x.get("version_id", 0))
                        decide_conflict(storage_controller, c, sorted_source_matches[-1])
                else:
                    # I have no entry from this source.
                    new_sources.append(c)
            else:
                id_matches = [x for x in conflict["existing_entries"] if "external_id" in x]
                if id_matches: 
                    exact_matches = []
                    for x in id_matches:
                        if str(c["external_id"]) == (x["external_id"]):
                            exact_matches.append(x)
                        
                    if exact_matches:
                        if "source_order" in exact_matches[0]:
                            c["source_order"] = exact_matches[0]["source_order"]
                        
                        if len(exact_matches) == 1:
                            decide_conflict(storage_controller, c, exact_matches[0])
                        else:
                            sorted_exact_matches = sorted(exact_matches, key=lambda x: x.get("version_id", 0))
                            decide_conflict(storage_controller, c, sorted_exact_matches[-1])
                    else:
                        # Found a new account associated with the user
                        logger.info("Found a new account associated to user {}".format(c["user_id"]))
                        with_source_ids = [x["source_order"] for x in exact_matches if "source_order" in x]
                        source_id = 0
                        if with_source_ids:
                            source_id = max(with_source_ids)

                        storage_controller.insert_new_user({**c, "source_id": source_id + 1})
                else:
                    new_sources.append(c)
                                        

        if new_sources:            
            location_per_source = {}
            for x in conflict["existing_entries"]:
                external = x.get("external_id", None)
                
                if external not in location_per_source:
                    location_per_source[external] = []
                
                location_per_source[external].append(x)
            
            locations = []
            for source_locations in location_per_source.values():
                locations = sorted(source_locations, key=lambda x: x.get("version_id", 0))
                locations.append(locations[-1])
                    
            all_locations = [x["location"] for x in [*locations, *new_sources]]     
            merged = location_parser.merge_georesults(all_locations)
            if merged:
                for x in new_sources:
                    logger.info("Found a new source of location for user {}".format(c["user_id"]))
                    
                    storage_controller.insert_new_user(x)
            else:
                # Found a conflict, the user must be removed from the normal pool
                logger.info("Found a location conflict associated to user {}. Locations found: {}".format(c["user_id"], all_locations))
                logger.info("Deleting all entries of user and moving data to conflict pile.")
                
                for x in conflict["existing_entries"]:
                    storage_controller.delete_user(x)
                    x.pop("_id")
                    storage_controller.store_conflict(x)
                
                for x in new_sources:
                    storage_controller.store_conflict(x)