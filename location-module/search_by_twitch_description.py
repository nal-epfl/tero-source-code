import sys
import redis
import json
import re
import requests

from pymongo import MongoClient
from time import sleep
from datetime import datetime
from logger import get_logger
from config import storage, redis_host, redis_port, redis_password, twitch_api_id, twitch_client_secret, mongo_host, mongo_port, mongo_user, mongo_password

from search_location import LocationParser
from twitch_descriptions.cliff_locator import CLIFFLocator
from twitch_descriptions.xponents_locator import XponentsLocator
from twitch_descriptions.mordecai_locator import MordecaiLocator

from nlp_utils import compare_nlp_locations, compare_cliff_xponents, compare_cliff_mordecai, compare_xponents_mordecai

twitch_logger = get_logger("search_twitch_description")


def get_api_token():
    params = {
        "client_id": twitch_api_id,
        "client_secret": twitch_client_secret,
        "grant_type": "client_credentials"
    }

    auth_url = "https://id.twitch.tv/oauth2/token?client_id={}&client_secret={}&grant_type={}".format(
        params['client_id'], params['client_secret'], params['grant_type'])

    try:
        auth_response =  requests.post(auth_url)
    except Exception:
        return False
    
    if auth_response and auth_response.status_code == 200:
        auth = auth_response.content.decode('utf-8')

        return json.loads(auth).get("access_token", "")


def get_header(token):
    return {
        "Client-ID": twitch_api_id,
        "Authorization": "Bearer {}".format(token)
    }


def get_request(url, headers=None, params=None):
    try:
        return requests.get(url, headers=headers, params=params)
    except Exception:
        return False


def get_users_information(users, token):    
    raw_data = []
    base_url = "https://api.twitch.tv/helix/users"

    counter = 0
    users_str = ""

    for idx, u in enumerate(users):
        if not counter:
            users_str = "id={}".format(u["twitch_id"])
            counter += 1
            continue

        if (idx + 1)%100 > 0 and idx < len(users) - 1:
            users_str = "{}&id={}".format(users_str, u["twitch_id"])
            counter += 1
        else:
            users_str = "{}&id={}".format(users_str, u["twitch_id"])

            twitch_logger.info("Querying twitch information. Current index: {}".format(idx))

            response = get_request("{}?{}".format(base_url, users_str), headers=get_header(token))
            if response and response.status_code == 200:
                response_json =  json.loads(response.text)
                
                raw_data.append(response_json)
            else:
                twitch_logger.info("Critical failure: saving pending users and exiting.")
                cache.sadd("twitch", *to_process[idx:])
                sys.exit()

            counter = 0
            sleep(sleep_time)
    
    return raw_data


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



def check_for_youtube(raw_data):
    to_search_youtube = []
    
    for result in raw_data:
        for user in result["data"]:
            if user["description"] and "youtube" in user["description"].lower():
                link = parse_youtube_link(user['description'])

                if link:
                    to_search_youtube.append({"twitch_id": user["id"], "youtube": link})

    return to_search_youtube

                   

if __name__ == '__main__':
    batch_size = 10000    
    path_to_storage = "{}/twitch".format(storage)
    sleep_time = 1

    twitch_logger.info("Started processing")
    
    mongo_client = MongoClient("mongodb://{}:{}/".format(mongo_host, mongo_port), username=mongo_user, password=mongo_password)
    cache = redis.Redis(host=redis_host, port=redis_port, db=0, password=redis_password)
    to_process = cache.spop("twitch", count=batch_size)
    n_to_process = len(to_process)

    twitch_logger.info("Users to process: {}".format(len(to_process)))

    if len(to_process) < 100:
        twitch_logger.info("Less than 100 users to search, stopping process.")
        sys.exit()
    
    to_process = [json.loads(x.decode("utf-8")) for x in to_process]

    token = get_api_token()
    raw_data = get_users_information(to_process, token)

    twitch_logger.info("Finished querying, writing results")
    now = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    with open("{}/{}_raw.json".format(path_to_storage, now), 'w+') as f:
        for r in raw_data:
            f.write(json.dumps(r) + "\n")

    to_search_youtube = check_for_youtube(raw_data)

    if to_search_youtube:
        all_scheduled = [json.loads(x.decode("utf8"))["twitch_id"] for x in cache.smembers("twitch_youtube_suspects")]

        for ta in to_search_youtube:
            if not cache.hexists("twitch_youtube_checked", ta["twitch_id"]) and ta["twitch_id"] not in set(all_scheduled):
                cache.sadd("twitch_youtube_suspects", json.dumps(ta))        

    users_found = set()
    all_locations = {}

    cliff_locator = CLIFFLocator()
    twitch_logger.info("Running CLIFF location")
    direct_insert_cliff, to_compare_cliff = cliff_locator.run(raw_data)
    twitch_logger.info("CLIFF results. Total detected: {}, to insert: {}, to compare: {}".format(len(direct_insert_cliff) + len(to_compare_cliff), len(direct_insert_cliff), len(to_compare_cliff)))

    for tc in to_compare_cliff:
        if tc["twitch_id"] not in all_locations:
            all_locations[tc["twitch_id"]] = []
        all_locations[tc["twitch_id"]].append({"source": "cliff", "location": tc})

    twitch_logger.info("Running Xponents location")
    xponents_locator = XponentsLocator()
    direct_insert_xponents, to_compare_xponents = xponents_locator.run(raw_data)
    twitch_logger.info("Xponents results. Total detected: {}, to insert: {}, to compare: {}".format(len(direct_insert_xponents) + len(to_compare_xponents), len(direct_insert_xponents), len(to_compare_xponents)))

    for tc in to_compare_xponents:
        if tc["twitch_id"] not in all_locations:
            all_locations[tc["twitch_id"]] = []
        all_locations[tc["twitch_id"]].append({"source": "xponents", "location": tc})

    direct_by_user = {}
    for l in [*direct_insert_cliff, *direct_insert_xponents]:
        if l["twitch_id"] not in direct_by_user:
            direct_by_user[l["twitch_id"]] = []

        direct_by_user[l["twitch_id"]].append(l)
        users_found.add(l["twitch_id"])

    twitch_logger.info("Unique users after merging: {}".format(len(direct_by_user.keys())))

    to_process = []
    with open("{}/{}_located-direct.json".format(path_to_storage, now), 'w+') as f:                      
        for l in direct_by_user.values():
            to_insert = None
            if len(l) > 1:
                comparison = compare_nlp_locations(l[0]["location"], l[1]["location"])
                if comparison == 0 or comparison == 1:
                    to_insert = l[0]
                elif comparison == 2:
                    to_insert = l[1]
            elif len(l) == 1:
                to_insert = l[0]

            if to_insert:            
                f.write(json.dumps(to_insert) + "\n")
                to_process.append(to_insert)
                users_found.add(to_insert["twitch_id"])

    to_insert_cliff_xponents, to_check_location = compare_cliff_xponents(to_compare_cliff, to_compare_xponents)
    twitch_logger.info("To insert after comparing: {}".format(len(to_insert_cliff_xponents)))

    with open("{}/{}_located-comparison.json".format(path_to_storage, now), 'w+') as f:                      
        for l in to_insert_cliff_xponents:
            f.write(json.dumps(l) + "\n")
            to_process.append(l)
            users_found.add(l["twitch_id"])

    location_parser = LocationParser()
    final_to_check = location_parser.check_locations(to_check_location)
    
    if final_to_check:
        twitch_logger.info("To insert after querying locations: {}".format(len(final_to_check)))
        with open("{}/{}_located-comparison.json".format(path_to_storage, now), 'a+') as f:                      
            for l in final_to_check:
                f.write(json.dumps(l) + "\n")
                to_process.append(l)
                users_found.add(l["twitch_id"])

    mordecai_locator = MordecaiLocator()
    to_compare_mordecai = mordecai_locator.run(raw_data)

    for tc in to_compare_mordecai:
        if tc["twitch_id"] not in all_locations:
            all_locations[tc["twitch_id"]] = []
        all_locations[tc["twitch_id"]].append({"source": "mordecai", "location": tc})

    twitch_logger.info("To compare from mordecai: {}".format(len(to_compare_mordecai)))

    to_insert_cliff_mordecai = compare_cliff_mordecai(to_compare_cliff, to_compare_mordecai)
    to_insert_xponents_mordecai = compare_xponents_mordecai(to_compare_xponents, to_compare_mordecai)
    
    twitch_logger.info("To insert from cliff+mordecai: {}".format(len(to_insert_cliff_mordecai)))
    twitch_logger.info("To compare from xponents+mordecai: {}".format(len(to_insert_xponents_mordecai)))
    
    mordecai_by_user = {}
    for l in [*to_insert_cliff_mordecai, *to_insert_xponents_mordecai]:
        if l["twitch_id"] not in mordecai_by_user:
            mordecai_by_user[l["twitch_id"]] = []

        mordecai_by_user[l["twitch_id"]].append(l)

    twitch_logger.info("Unique users after merging mordecai: {}".format(len(mordecai_by_user.keys())))

    with open("{}/{}_located-mordecai.json".format(path_to_storage, now), 'w+') as f:                      
        for l in mordecai_by_user.values():
            to_insert = None
            loc1 = l[0]["location"][0] if isinstance(l[0]["location"], list) else l[0]["location"]

            if len(l) > 1:
                loc2 = l[1]["location"][0] if isinstance(l[1]["location"], list) else l[1]["location"]

                comparison = compare_nlp_locations(loc1, loc2)
                if comparison == 0 or comparison == 1:
                    l[0]["location"] = loc1
                    to_insert = l[0]
                elif comparison == 2:
                    l[1]["location"] = loc2
                    to_insert = l[1]
            elif len(l) == 1:
                l[0]["location"] = loc1
                to_insert = l[0]

            if to_insert:
                f.write("{}\n".format(json.dumps(to_insert)))
                to_process.append(to_insert)
                users_found.add(to_insert["twitch_id"])

    for twitch_id, locations in all_locations.items():
        if twitch_id not in users_found:
            cache.hset("locations_lost", twitch_id, json.dumps(locations))

    twitch_logger.info("Writing results. Number of users to locate: {}".format(len(to_process)))
    
    if to_process:
        for l in to_process:          
            cache.sadd("to_locate", json.dumps(l))
            cache.sadd("found_twitch", l["twitch_id"])

    twitch_logger.info("Storing statistics")
    with_description = 0
    
    for result in raw_data:
        for user in result['data']:
            if user["description"].strip():
                with_description += 1

    mongo_client.user_stats.descriptions.insert_one({"ts": datetime.now().timestamp(), "input_users": n_to_process, "users_with_descriptions": with_description, "found": {"cliff": len(direct_insert_cliff), "xponents": len(direct_insert_xponents), "cliff_xponents": len(to_insert_cliff_xponents), "cliff_mordecai": len(to_insert_cliff_mordecai), "xponents_mordecai": len(to_insert_xponents_mordecai)}})

    twitch_logger.info("Finishing process")
