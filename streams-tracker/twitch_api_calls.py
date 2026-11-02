import json
import requests

from config import twitch_api_id, twitch_client_secret


def get_request(url, headers=None, params=None):
    try:
        return requests.get(url, headers=headers, params=params)
    except Exception:
        return False


def post_request(url):
    try:
        return requests.post(url)
    except Exception:
        return False


def get_api_token():
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


def get_header(token):
    return {
        "Client-ID": twitch_api_id,
        "Authorization": "Bearer {}".format(token)
    }
