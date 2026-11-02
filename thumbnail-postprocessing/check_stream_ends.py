import json
from pymongo import MongoClient
from tqdm import tqdm
from config import mongo_host, mongo_port, mongo_user, mongo_password

mongo_client = MongoClient("mongodb://{}:{}/".format(mongo_host, mongo_port), username=mongo_user, password=mongo_password)
thumbnail_db = mongo_client.thumbnail_data

all_stream_ends = {}

for col in tqdm(mongo_client.thumbnail_data.list_collection_names()):
    for x in thumbnail_db[col].find():
        if x["stream_id"] not in all_stream_ends:
            all_stream_ends[x["stream_id"]] = {"start": int(x["date"]), "end": int(x["date"])}
        
        all_stream_ends[x["stream_id"]]["start"] = min(all_stream_ends[x["stream_id"]]["start"], int(x["date"]))
        all_stream_ends[x["stream_id"]]["end"] = max(all_stream_ends[x["stream_id"]]["start"], int(x["date"]))

with open("all_stream_ends.json", "w+") as f:
    for stream_id, data in all_stream_ends.items():
        f.write("{}\n".format(json.dumps({"stream_id": stream_id, **data})))