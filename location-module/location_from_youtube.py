import redis
import json
import pycountry

from config import redis_host, redis_port, redis_password
from logger import get_logger


def format_country(country_data):
    return {"country_code": country_data.alpha_2.lower(), "country": country_data.name}


def parse_youtube(data):
    if "items" not in data['youtube']:
        return
    
    for item in data['youtube']['items']:         
        if "snippet" in item:                
            if "country" in item['snippet']:
                try:
                    country_data = pycountry.countries.lookup(item['snippet']['country'].lower())
                    return {"twitch_id": data["data"]['twitch_id'], "youtube_id": item['id'], "location": format_country(country_data)}
                except LookupError:
                    continue


if __name__ == "__main__":
    logger = get_logger("locations_youtube")

    logger.info("Starting process...")

    cache = redis.Redis(host=redis_host, port=redis_port, password=redis_password)

    processed = 0
    correctly_parsed = 0
    to_process = cache.hlen("twitch_youtube_checked")

    for user_id, data in cache.hgetall("twitch_youtube_checked").items():
        user_id = user_id.decode("utf8")
        data = json.loads(data.decode("utf8"))

        if cache.hexists("youtube_locations", user_id):
            continue

        cache.hset("youtube_locations", user_id, json.dumps(data))
        parsed = parse_youtube(data)
        processed += 1

        if parsed:
            cache.sadd("to_locate", json.dumps(parsed))
            cache.sadd("found_youtube", parsed["twitch_id"])
            correctly_parsed += 1

    logger.info("Finishing process. Processed: {}, Parsed: {}".format(processed, correctly_parsed))