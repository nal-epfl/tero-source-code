import re
import sys
import requests
import json
import redis

from time import sleep
from config import daily_quota, youtube_api_key, redis_host, redis_port, redis_password, youtube_safety_factor
from logger import get_logger


def parse_youtube_link(link):
    m = re.search(r"youtube.com/user/(?P<name>(\w+-*\w+)+)", link)
    if m:
        return {"name": m.group("name")}

    m = re.search(r"youtube.com/c/(?P<name>(\w+-*\w+)+)", link)
    if m:
        return {"name": m.group("name")}

    m = re.search(r"youtube.com/channel/(?P<id>(\w+-*\w+)+)", link)
    if m:
        return {"id": m.group("id")}

    m = re.search(r"youtube.com/(?P<name>(\w+-*\w+)+)", link)
    if m:
        return {"name": m.group("name")}


def get_channel_by_username(user_name):
    url = "https://youtube.googleapis.com/youtube/v3/channels?part=snippet&forUsername={}" \
          "&key={}".format(user_name, youtube_api_key)

    response = requests.request("GET", url)

    if response.status_code == 200:
        return json.loads(response.text)
    else:
        print("[{}] Searching user in Youtube failed. User: {}".format(response.status_code, user_name))



def get_channel_by_id(channel_id):
    url = "https://youtube.googleapis.com/youtube/v3/channels?part=snippet&id={}" \
          "&key={}".format(channel_id, youtube_api_key)

    response = requests.request("GET", url)

    if response.status_code == 200:
        return json.loads(response.text)
    else:
        print("[{}] Searching for channel using id in Youtube failed. User id: {}".format(response.status_code, channel_id))
   

if __name__ == '__main__':
    logger = get_logger("youtube_twitch_connection")
    
    batch_size = 1000

    cache = redis.Redis(host=redis_host, port=redis_port, password=redis_password)
    quota_used_list = [int(x.decode("utf8")) for x in cache.smembers("youtube_quota")]

    quota_used = quota_used_list[0] if quota_used_list else 0
    logger.info("Starting processing. Current quota used: {}".format(quota_used))

    if quota_used > daily_quota*youtube_safety_factor:
        sys.exit(0)

    links_to_query = [json.loads(x.decode("utf8")) for x in cache.spop("twitch_youtube_suspects", count=batch_size)]
    checked_idx = 0
    extra_quota_used = 0

    for idx, link in enumerate(links_to_query):
        if quota_used > daily_quota*youtube_safety_factor:
            logger.info("Ending prematurely, quota exceeded.")
            break

        checked_idx += 1

        if cache.hexists("twitch_youtube_checked", link["twitch_id"]):
            continue

        if "id" in link["youtube"]:
            channel_info = get_channel_by_id(link["youtube"]['id'])
            quota_used += 1
            extra_quota_used += 1

            if channel_info:
                cache.hset("twitch_youtube_checked", link["twitch_id"], json.dumps({'data': link, "youtube": channel_info}))
            
        if "name" in link["youtube"]:
            channel_info = get_channel_by_username(link["youtube"]['name'])
            quota_used += 1
            extra_quota_used += 1

            if channel_info:
                cache.hset("twitch_youtube_checked", link["twitch_id"], json.dumps({'data':link, "youtube": channel_info}))
            
        sleep(2)
        if idx%100 == 0:
            logger.info("Processed: {}/{}".format(idx, len(links_to_query)))

    if links_to_query[checked_idx:]:
        logger.info("Returning some entries: {}".format(len(links_to_query[checked_idx:])))

        for x in links_to_query[checked_idx:]:
            cache.sadd("twitch_youtube_suspects", json.dumps(x))
    
    old_quota = [int(x.decode("utf8")) for x in cache.spop("youtube_quota", count=1)]
    old_quota = old_quota[0] if old_quota else 0
       
    logger.info("Inserting quota used: {}".format(old_quota + extra_quota_used))
    cache.sadd("youtube_quota", old_quota + extra_quota_used)
        

    