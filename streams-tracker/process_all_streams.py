import redis
import json
import sys
import hashlib
import hmac

from pymongo import MongoClient
from datetime import datetime
from config import redis_host, redis_port, redis_password, mongo_host, mongo_port, mongo_user, mongo_password, secret_key, path_to_storage
from logger import get_logger


chunk_size = 5

def compress_entries(entries):
    summarized_stream = []
    
    current_game = ""
    start = datetime(year=2021, month=5, day=1).timestamp()
    last_ts = datetime(year=2021, month=5, day=1).timestamp()

    for entry in entries:
        if not current_game:
            current_game = entry["game"]
            start = entry["ts"]
            continue
                            
        if entry["game"] != current_game:
            summarized_stream.append({"game": current_game, "start": start, "end": last_ts})
            current_game = entry["game"]
            start = entry["ts"]

        last_ts = entry["ts"]

    if last_ts != start:
        summarized_stream.append({"game": current_game, "start": start, "end": last_ts})

    return summarized_stream


if __name__ == "__main__":
    logger = get_logger("process_all_streams")
    
    logger.info('Starting process...')
    cache = redis.Redis(host=redis_host, port=redis_port, password=redis_password)
    mongo_client = MongoClient('mongodb://{}:{}/'.format(mongo_host, mongo_port), username=mongo_user, password=mongo_password)

    users_with_location = set([x.decode("utf8") for x in cache.spop("to_probe", count=cache.scard("to_probe"))])
    files_to_process = sorted([x.decode("utf8") for x in cache.spop("new_stream_files", count=cache.scard("new_stream_files"))])

    logger.info("Files to process: {}".format(len(files_to_process)))
    if len(files_to_process) < 5:
        sys.exit(0)

    file_chunks = [files_to_process[i:i + chunk_size] for i in range(0, len(files_to_process), chunk_size)]

    for chunk in file_chunks:
        data_to_keep = {}
        finished_streams = set()
        currently_active = set()
        
        for file_name in chunk:
            with open("{}/{}".format(path_to_storage, file_name), "r") as f:
                for l in f:
                    data = json.loads(l.strip())

                    for stream in data["data"]:
                        if stream["user_id"] in users_with_location:
                            if stream["id"] not in data_to_keep:
                                data_to_keep[stream["id"]] = []
                            
                            data_to_keep[stream["id"]].append({"user_id": stream["user_id"], "ts": data["timestamp"], "game": stream["game_id"]})
                            currently_active.add(stream["id"])

        old_active = set([x.decode("utf8") for x in cache.smembers("old_streams")])
        finished_streams.update(old_active - currently_active)

        cache.spop("old_streams", count=cache.scard("old_streams"))
        cache.sadd("old_streams", *list(currently_active))

        for stream_id, data in data_to_keep.items():
            old_data = []

            if cache.hexists("old_streams_data", stream_id):
                old_data = json.loads(cache.hget("old_streams_data", stream_id).decode("utf8"))["data"]

            old_data.extend(data)
            cache.hset("old_streams_data", stream_id, json.dumps({"data": data}))

        # Now, for the streams that have finished:        
        for stream_id in finished_streams:
            # Fetch the stream's old data (stored from previous runs of the script)
            if cache.hexists("old_streams_data", stream_id):
                data = json.loads(cache.hget("old_streams_data", stream_id))["data"]
                if not data:
                    continue

                sorted_by_date = sorted(data, key=lambda x: x["ts"])               

                compressed = compress_entries(sorted_by_date)

                encoded_stream_id = hmac.new(secret_key.encode("utf-8"), stream_id.encode("utf-8"), hashlib.sha1).hexdigest()
                encoded_user_id = hmac.new(secret_key.encode("utf-8"), data[0]["user_id"].encode("utf-8"), hashlib.sha1).hexdigest()

                mongo_client.streams.coarse_data.insert_one({"user_id": encoded_user_id, "stream_id": encoded_stream_id, "changes": len(compressed)-1, "games": compressed})
                cache.sadd("streams_new_coarse_grained", encoded_stream_id)

                # Once the data is in mongo, delete it from the cache
                cache.hdel("old_streams_data", stream_id)
