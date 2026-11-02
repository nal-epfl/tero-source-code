import json
import redis
import hashlib
import hmac

from storage.mongo_controller import MongoController

from datetime import datetime
from config import redis_host, redis_port, redis_password, secret_key
from logger import get_logger
from twitch_api_calls import get_api_token, get_header, get_request
from stream_gatherer import check_rate_limits


def format_stream_data(data):
    now = datetime.now().timestamp()
    game_names = {}
    stream_starts = {}

    list_to_store = []

    for stream in data["data"]:
        encoded_user = hmac.new(secret_key.encode("utf-8"), stream["user_id"].encode("utf-8"), hashlib.sha1).hexdigest()
        encoded_stream = hmac.new(secret_key.encode("utf-8"), stream["id"].encode("utf-8"), hashlib.sha1).hexdigest()

        list_to_store.append({"stream_id": encoded_stream, "ts": now, "viewers": stream["viewer_count"], "game": stream["game_id"], "title": stream["title"]})
        
        game_names[stream["game_id"]] = stream["game_name"]
        stream_starts[encoded_stream] = {"user_id": encoded_user, "stream_id": encoded_stream, "start": stream["started_at"]}

    return stream_starts, game_names, list_to_store


def get_current_streams(token, storage):
    logger = get_logger("track_current")
    logger.info('Starting gathering')

    cache = redis.Redis(host=redis_host, port=redis_port, password=redis_password)
    users = []

    streams_last_time = {}
    for stream_data in cache.spop("active_streams", count=cache.scard("active_streams")):
        data = json.loads(stream_data.decode("utf8"))
        
        streams_last_time[data["twitch_id"]] = data["stream_id"]

    active_users = {}

    for v in cache.hgetall("current_probes").values():
        decoded_v = json.loads(v.decode("utf8"))

        if "twitch_id" in decoded_v:
            users.append(decoded_v["twitch_id"])

        if decoded_v["twitch_id"] in streams_last_time:
            streams_last_time.pop(decoded_v["twitch_id"])

        active_users[decoded_v["twitch_id"]] = decoded_v["id"]
    
    for twitch_id, stream in streams_last_time.items():
        user_id = hmac.new(secret_key.encode("utf-8"), twitch_id.encode("utf-8"), hashlib.sha1).hexdigest()
        
        cache.sadd("finished_streams", json.dumps({"user_id": user_id, "stream_id": stream}))

    for current_user, stream in active_users.items():
        cache.sadd("active_streams", json.dumps({"twitch_id": current_user, "stream_id": stream}))

    if not users:
        return

    logger.info('Current users: {}'.format(len(users)))

    streams_url = "https://api.twitch.tv/helix/streams?user_id={}".format(users[0])
    counter = 0
    total = 0

    for user in users[1:]:    
        if counter >= 99:
            response = get_request(streams_url, headers=get_header(token))
            if response and response.status_code == 200:
                response_json =  json.loads(response.text)
                
                stream_metadata, game_names, list_to_store = format_stream_data(response_json)
                storage.save_stream_metadata(stream_metadata)
                storage.save_game_name_mapping(game_names)
                storage.save_stream_data(list_to_store)
                
                logger.info('Processing: {}/{}'.format(100*total, len(users)))
            
            check_rate_limits(logger, response)

            counter = 0
            total += 1
            streams_url = "https://api.twitch.tv/helix/streams?user_id={}".format(user)
        
            continue
            
        streams_url = "{}&user_id={}".format(streams_url, user)
        counter += 1
        
    response = get_request(streams_url, headers=get_header(token))
    if response and response.status_code == 200:
        response_json =  json.loads(response.text)
        
        if response and response.status_code == 200:
            response_json =  json.loads(response.text)    
            stream_metadata, game_names, list_to_store = format_stream_data(response_json)
            
            storage.save_stream_metadata(stream_metadata)
            storage.save_game_name_mapping(game_names)
            storage.save_stream_data(list_to_store)

            logger.info('Processing: {}/{}'.format(100*total + counter, len(users)))

    logger.info('Finished gathering')


def main():
    token = get_api_token()
    storage = MongoController()
    
    get_current_streams(token, storage)


if __name__ == '__main__':
    main()

