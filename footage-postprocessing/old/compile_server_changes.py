from pymongo import MongoClient, DESCENDING
from config import  mongo_host, mongo_password, mongo_user

from utils import process_user_changes




    




mongo_client = MongoClient("mongodb://{}:27017/".format(mongo_host),
                            username=mongo_user,
                            password=mongo_password)


changes_per_user_game = {}

for file_name in os.listdir(path):
    game_id = file_name.split("_")[0]
    user_id = file_name.split("_")[-1].replace(".json", "")
    
    with open("{}/{}".format(path, file_name), "r") as f:
        for l in f:
            data = json.loads(l.strip())
            if data["transition"] > qoe_band*2:
                if game_id not in changes_per_user_game:
                    changes_per_user_game[game_id] = {}

                if user_id not in changes_per_user_game[game_id]:
                    changes_per_user_game[game_id][user_id] = []

                changes_per_user_game[game_id][user_id].append(data)


# for game_id in changes_per_user_game.keys():
#     with open("location-changes-report-{}.json".format(game_id), "w+") as f:
#         for user_id, changes in changes_per_user_game[game_id].items():
#             flattened_subsequences, group_transitions = process_user(game_id, user_id, changes)     
#             for x in flattened_subsequences:
#                 x.pop("streams")       
#             f.write("{}\n".format(json.dumps({"user_id": user_id, "groups": flattened_subsequences, "server_changes": [{"groups": list(k), "server_change": v} for k, v in group_transitions.items()]})))


# reports_path = "location-changes-reports"
# streams_with_server_changes_per_user_game = {}

# for file in os.listdir(reports_path):
#     game_id = file.split("-")[-1].replace(".json", "")
#     streams_with_server_changes_per_user_game[game_id] = {}

#     with open("{}/{}".format(reports_path, file), "r") as f:
#         for l in f:
#             data = json.loads(l.strip())
#             streams = {}

#             for l in data["server_changes"]:
#                 if l["server_change"]["server_change"]:
#                     for s in l["server_change"]["streams"][0]: 
#                         streams[s] = l["server_change"]["streams"][1]
            
#             if streams:
#                 streams_with_server_changes_per_user_game[game_id][data["user_id"]] = streams



# for qoe_band in [5, 8, 10, 15, 20, 25, 30, 35, 40]:
#     if not os.path.isdir("{}/qoe{}".format(spike_server_change_path, qoe_band)):
#         os.makedirs("{}/qoe{}".format(spike_server_change_path, qoe_band))

#     for game_id, data_per_user in tqdm(streams_with_server_changes_per_user_game.items()):
#         with open("{}/qoe{}/spikes-location-changes-{}.json".format(spike_server_change_path, qoe_band, game_id), "w+") as f:
#             for user_id, streams in data_per_user.items():
#                 for stream_id, dates in streams.items():               
#                     spikes_contained = [x for x in mongo_client.events["qoe_{}".format(qoe_band)].find({"game_id": game_id, "user_id": user_id, "stream_id": stream_id}, projection={"_id": False})]
#                     f.write("{}\n".format(json.dumps({"game_id": game_id, "user_id": user_id, "stream_id": stream_id, "change_dates": dates, "spikes": spikes_contained})))