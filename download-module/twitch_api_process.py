import json
import requests
import redis

from time import sleep

from config import redis_host, redis_password, redis_port, games_to_probe, twitch_api_id, twitch_client_secret, rate_limit_safety_factor, base_path
from logger import get_logger


def post_request(url):
    try:
        return requests.post(url)
    except Exception:
        return False



class TwitchAPIProcess:
    def __init__(self):
        super().__init__()
        self.logger = get_logger("twitch_logger", 'twitch')
        self.storage = redis.Redis(host=redis_host, port=redis_port, db=0, password=redis_password)
        self.token = self.get_api_token()
        self.online = []
        self.offline = []

        with open("{}/data/games.json".format(base_path), "r") as f:
            self.games_mapping = json.load(f)


    def check_rate_limits(self, streams_response):
        available_tokens = int(streams_response.headers.get('ratelimit-remaining'))
        total_tokens = int(streams_response.headers.get('ratelimit-limit'))

        self.logger.info('Tokens: Available {}, Total {}'.format(available_tokens, total_tokens))

        if rate_limit_safety_factor * total_tokens > available_tokens:
            # You are under the safety level, it's better to backoff
            self.logger.info("Under safety level, I'll sleep for 5 seconds")
            sleep(5)


    def get_api_token(self):
        params = {
            "client_id": twitch_api_id,
            "client_secret": twitch_client_secret,
            "grant_type": "client_credentials"
        }

        auth_url = "https://id.twitch.tv/oauth2/token?client_id={}&client_secret={}&grant_type={}".format(
            params['client_id'], params['client_secret'], params['grant_type'])

        auth_response = post_request(auth_url)
        if auth_response and auth_response.status_code == 200:
            auth = auth_response.content.decode('utf-8')

            return json.loads(auth).get("access_token", "")
        elif auth_response and auth_response.status_code != 200:
            self.logger.error("Error getting token. Status code: {}".format(auth_response.status_code))
        else:
            self.logger.error("Empty response")


    def get_header(self):
        return {
            "Client-ID": twitch_api_id,
            "Authorization": "Bearer {}".format(self.token)
        }

    def loop(self):
        while True:
            to_query = self.storage.spop("twitch_api_to_query", count=self.storage.scard("twitch_api_to_query"))

            if not to_query:
                sleep(5)
                continue
            
            to_query = [x.decode("utf-8") for x in to_query]

            chunk_size = 100
            data_chunks = [to_query[i:i + chunk_size] for i in range(0, len(to_query), chunk_size)]

            for chunk in data_chunks:
                new_users_data = self.query_users(chunk)
                        
                if new_users_data:
                    response = json.loads(new_users_data)

                    self.separate_online_offline(chunk, response)
                else: #Log something here!
                    self.logger.info("Query failed! Chunk: {}".format(chunk))
            
            if self.online:
                self.storage.sadd("twitch_api_online", *self.online)
                self.online = []
            
            if self.offline:
                self.storage.sadd("twitch_api_offline", *self.offline)
                self.offline = []
        

    def query_users(self, chunk):
        users_string = ""

        for user in chunk:
            users_string = "{}user_id={}&".format(users_string, user)

        users_string = users_string[:-1]

        users_url = "https://api.twitch.tv/helix/streams?{}".format(users_string)
        response = self.run_query(users_url)

        if response and response.status_code == 200:
            self.check_rate_limits(response)

            return response.content.decode('utf-8')
    

    def run_query(self, url):
        try:
            return requests.get(url, headers=self.get_header(), params={}, timeout=5)
        except Exception as e:
            self.logger.info("Exception: {}".format(e))
            return False    


    def separate_online_offline(self, chunk, response):    
        online = set()
        users = set(chunk)

        for stream in response['data']:
            # Add filter by game here
            if stream['game_id'] in games_to_probe:
                game_id = self.games_mapping.get(stream['game_id'], None)

                if game_id:
                    online.add(stream['user_id'])

                    self.online.append(json.dumps({
                        "stream_id": stream['id'],
                        "user_id": stream['user_id'],
                        "game_id": game_id["id"],
                        "url": stream['thumbnail_url']
                    }))

        self.offline.extend(list(users - online))


if __name__ == '__main__':
    api_process = TwitchAPIProcess()
    api_process.loop()
