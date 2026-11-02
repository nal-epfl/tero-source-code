import os
from dotenv import load_dotenv

load_dotenv()

safety_factor = 0.4

redis_host = os.getenv('REDIS_HOST')
redis_password = os.getenv('REDIS_PASSWORD')
redis_port = os.getenv('REDIS_PORT')

base_path = "locate-users-online"

sleep_on_failure = 60
sleep_on_success = 1

twitter_bearer_token = os.getenv('TWITTER_BEARER_TOKEN')
path_to_profiles = "storage/steam_profiles"

storage = "data/storage"

steam_api_key = os.getenv('STEAM_API_KEY')

twitch_api_id = os.getenv('TWITCH_API_ID')
twitch_client_secret = os.getenv('TWITCH_CLIENT_SECRET')

cliff_host = "localhost"
cliff_port = "8080"

mongo_host = os.getenv('MONGO_HOST')
mongo_user = os.getenv('MONGO_USER')
mongo_password = os.getenv('MONGO_PASSWORD')
mongo_port = os.getenv('MONGO_PORT')

secret_key = os.getenv('SECRET_KEY')

xponents_host = "localhost"

s3_url = os.getenv('S3_URL')
rw_access_key = os.getenv('RW_ACCESS_KEY')
rw_secret_key = os.getenv('RW_SECRET_KEY')
bucket_name = os.getenv('BUCKET_NAME')