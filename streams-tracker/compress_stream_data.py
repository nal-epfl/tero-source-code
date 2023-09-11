import json
import os
import sys
import redis

from datetime import datetime
from pymongo import MongoClient
from logger import get_logger
from config import redis_host, redis_port, redis_password, mongo_host, mongo_port, mongo_user, mongo_password, fine_grained_tmp_storage



if __name__ == "__main__":
    logger = get_logger("compress_stream_data")
    
    cache = redis.Redis(host=redis_host, port=redis_port, password=redis_password)
    mongo_client = MongoClient('mongodb://{}:{}/'.format(mongo_host, mongo_port), username=mongo_user, password=mongo_password)
    
    logger.info("Fetching finished streams.")
    streams_to_compress = [json.loads(x.decode("utf8")) for x in cache.spop("finished_streams", count=cache.scard("finished_streams"))]
    
    logger.info("Finished streams: {}".format(len(streams_to_compress)))
    
    if not streams_to_compress:
        sys.exit(0)
    
    logger.info("Processing streams to store...")
    for stream in streams_to_compress:
        start = [x for x in mongo_client.streams.data.find({"stream_id": stream["stream_id"]}, projection={"_id": False}).sort("ts").limit(1)]

        if not start:
            continue

        start = start[0]
        start_day = datetime.fromtimestamp(start["ts"]).strftime("%Y-%m-%d")

        if not os.path.isdir("{}/{}".format(fine_grained_tmp_storage, start_day)):
            os.makedirs("{}/{}".format(fine_grained_tmp_storage, start_day))

        summarized_stream = []
        current_game = ""       
        last_ts = datetime(year=2021, month=5, day=1).timestamp()

        file_name = "{}/{}/stream_data-{}.json".format(fine_grained_tmp_storage, start_day, stream["stream_id"])
        with open(file_name, "w+") as f:
            for entry in mongo_client.streams.data.find({"stream_id": stream["stream_id"]}, projection={"_id": False}).sort("ts"):
                if not current_game:
                    current_game = entry["game"]
                    start = entry["ts"]
                    continue
                            
                if entry["game"] != current_game:
                    summarized_stream.append({"game": current_game, "start": start, "end": last_ts})
                    current_game = entry["game"]
                    start = entry["ts"]

                last_ts = entry["ts"]
                f.write("{}\n".format(json.dumps(entry)))

        if last_ts != start:
            summarized_stream.append({"game": current_game, "start": start, "end": last_ts})

        mongo_client.streams.summaries.insert_one({"user_id": stream["user_id"], "stream_id": stream["stream_id"], "changes": len(summarized_stream)-1, "games": summarized_stream})
        mongo_client.streams.data.delete_many({"stream_id": stream["stream_id"]})    
        
        
