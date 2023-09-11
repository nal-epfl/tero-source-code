import json
import requests
import redis

from pymongo import MongoClient
from time import sleep
from subprocess import run, PIPE
from logger import get_logger, base_path
from datetime import datetime
from os import listdir, remove
from os.path import join, isfile
from config import redis_host, redis_port, steam_api_key, path_to_profiles, storage, redis_password, mongo_host, mongo_port, mongo_user, mongo_password


logger = get_logger("search_steam")


def get_request(url, headers=None, params=None):
    try:
        return requests.get(url, headers=headers, params=params)
    except Exception:
        return False


def get_steam_id_from_name(twitch_name):
    url = "http://api.steampowered.com/ISteamUser/ResolveVanityURL/v0001/?key={}&vanityurl={}".format(steam_api_key,
                                                                                          twitch_name)
    user_response = get_request(url)

    if user_response and user_response.status_code == 200:
        user = json.loads(user_response.content.decode('utf-8'))

        if user.get('response', {}).get('success') == 1:
            return user.get('response', {}).get('steamid', 0)

    return False


def get_user_info_from_id(steam_id):
    url = "http://api.steampowered.com/ISteamUser/GetPlayerSummaries/v0002/?key={}&steamids={}".format(steam_api_key,
                                                                                                       steam_id)
    user_response = get_request(url)

    if user_response and user_response.status_code == 200:
        user = json.loads(user_response.content.decode('utf-8'))

        return user.get('response', {}).get('players', [])


def get_location_from_info(user):
    country = user.get('loccountrycode', '')
    state = user.get('locstatecode', '')
    city = user.get('loccityid', '')

    result = run(["ruby", "{}/thirdparty/get_steam_location.rb".format(base_path), country, state, str(city)],
                    stdout=PIPE, universal_newlines=True)
    return json.loads(result.stdout)


def get_aliases(user_url):
    aliases_url = "{}/ajaxaliases".format(user_url)
    aliases_response = get_request(aliases_url)

    if aliases_response and aliases_response.status_code == 200:
        return json.loads(aliases_response.content.decode('utf-8'))


def download_profile_page(user_name, profile_url):
    profile_page = requests.get(profile_url)

    if profile_page.status_code == 200:
        open('{}/{}.html'.format(path_to_profiles, user_name), 'wb').write(profile_page.content)        


if __name__ == '__main__':
    batch_size = 100   
    sleep_time = 1
    path_to_storage = "{}/steam".format(storage)
    
    logger.info("Started processing")

    mongo_client = MongoClient("mongodb://{}:{}/".format(mongo_host, mongo_port), username=mongo_user, password=mongo_password)
    cache = redis.Redis(host=redis_host, port=redis_port, db=0, password=redis_password)
    to_process = cache.spop("steam", count=batch_size)
    n_to_process = len(to_process)

    logger.info("Users to process: {}".format(len(to_process)))

    to_save = {"found_directly": [], "found_webpage": []}
    
    for idx, user in enumerate(to_process):
        if idx % 10 == 0:
            logger.info("Processing {}/{}".format(idx, len(to_process)))

        data = json.loads(user)
        user_id = get_steam_id_from_name(data['twitch_name'])

        if user_id:
            steam_data = get_user_info_from_id(user_id)

            if not steam_data:
                continue

            steam_data = steam_data[0]
            aliases = get_aliases(steam_data['profileurl'])
            if not aliases:
                continue

            confirmed_with_alias = False

            for alias in aliases:
                if "twitch" in alias['newname'].lower():
                    location = get_location_from_info(steam_data)

                    if location:
                        to_save["found_directly"].append({**data, 'steam_name': steam_data['personaname'], 'steam_id': steam_data['steamid'],'location': location['map_search_string']})
                        confirmed_with_alias = True
                        break
                
            if not confirmed_with_alias:
                download_profile_page(data['twitch_name'], steam_data['profileurl'])
                to_save["found_webpage"].append({**data, 'steam_data': steam_data})
            
        sleep(sleep_time)
    
    logger.info("Finished querying, writing results")
    
    now = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    with open("{}/{}_raw.json".format(path_to_storage, now), 'w+') as f:
        json.dump(to_save, f)

    to_process = []
    for user in to_save["found_directly"]:
        to_process.append(json.dumps({'twitch_id': user['twitch_id'], 'location': user['location'], 'steam_id': user['steam_id']}))


    for user in to_save["found_webpage"]:
        try:
            user_profile = open('{}/{}.html'.format(path_to_profiles, user['twitch_name']), 'rb').read().decode().lower()
        except Exception as e:
            print(e)
            continue
        
        to_find =  'twitch.tv/{}'.format(data['twitch_name'].lower())

        if to_find in user_profile:
            location = get_location_from_info(data['steam_data'])

            if location:
                to_process.append(json.dumps({'twitch_id': data['twitch_id'], 'location': location['map_search_string'], 'steam_id': user['steam_id']}))        

    logger.info("Found {} users to locate".format(len(to_process)))

    old_processed = [x.decode("utf8") for x in cache.spop("processed_steam", count=1)]
    old_processed = int(old_processed[0]) if old_processed else 0

    logger.info("Storing results and statistics")

    if to_process:
        cache.sadd("to_locate", *to_process)
        mongo_client.user_stats.steam.insert_one({"ts": datetime.now().timestamp(), "input_users": old_processed+n_to_process, "found": len(to_process)})
    else:
        cache.sadd("processed_steam", old_processed+n_to_process)

    files_to_delete = [join(path_to_profiles, x) for x in listdir(path_to_profiles)]

    logger.info("Deleting downloaded profile pages")    

    for ftd in files_to_delete:
        if isfile(ftd):
            remove(ftd)
    
    logger.info("Finishing process")