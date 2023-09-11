import json
import datetime

from storage.local_controller import LocalController
from time import sleep

from config import safety_factor, path_to_storage, redis_host, redis_port, redis_password
from logger import get_logger
from twitch_api_calls import get_api_token, get_header, get_request


def check_rate_limits(logger, streams_response):
    available_tokens = int(streams_response.headers.get('ratelimit-remaining'))
    total_tokens = int(streams_response.headers.get('ratelimit-limit'))

    logger.info('Tokens: Available {}, Total {}'.format(available_tokens, total_tokens))

    if safety_factor * total_tokens > available_tokens:
        # You are under the safety level, it's better to backoff
        sleep(5)


def get_all_streams(token, storage):
    logger = get_logger("streams_gatherer")

    logger.info('Starting gathering')

    streams_url = "https://api.twitch.tv/helix/streams?first=100"
    streams_response = get_request(streams_url, headers=get_header(token))

    if streams_response:
        if streams_response.status_code == 429:
            # Worst case scenario: you managed to exceed the rate limit, backoff for a long time
            sleep(60)

        if streams_response.status_code != 200:
            logger.debug("Anomalous exit: code {}".format(streams_response.status_code))

            return

        check_rate_limits(logger, streams_response)
        streams = json.loads(streams_response.content.decode('utf-8'))

        top_streams = streams.get('data')
        next_pointer = streams['pagination'].get('cursor', '')
        data_to_save = {'data': top_streams, 'timestamp': datetime.datetime.utcnow().timestamp()}
        users = [{"id": x["user_id"], "name": x["user_login"]} for x in top_streams]        

        storage.save_streams(data_to_save)
        storage.save_users(users)

        while next_pointer:
            streams_url_continuation = "https://api.twitch.tv/helix/streams?" \
                                       "first=100&after={}".format(next_pointer)
            streams_response = get_request(streams_url_continuation, headers=get_header(token))

            if streams_response:
                if streams_response.status_code == 429:
                    # Worst case scenario: you managed to exceed the rate limit, backoff for a long time
                    sleep(60)

                if streams_response.status_code != 200:
                    logger.debug("Anomalous exit: code {}".format(streams_response.status_code))
                    return

                check_rate_limits(logger, streams_response)
                streams = json.loads(streams_response.content.decode('utf-8'))
                next_pointer = streams['pagination'].get('cursor', '')

                data_to_save = {'data': streams.get('data'), 'timestamp': datetime.datetime.utcnow().timestamp()}
                users = [{"id": x["user_id"], "name": x["user_login"]} for x in streams.get('data')]

                storage.save_streams(data_to_save)
                storage.save_users(users)
                

        # Signal that the gathering has finished
        logger.info('Finished gathering')
        storage.finish()


def main():
    token = get_api_token()
    storage = LocalController(path_to_storage, {"host": redis_host, "port": redis_port, "password": redis_password})
    
    get_all_streams(token, storage)


if __name__ == '__main__':
    main()

