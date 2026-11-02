import os
from dotenv import load_dotenv

load_dotenv()

safety_factor = 0.7

twitch_api_id = os.getenv('TWITCH_API_ID')
twitch_client_secret = os.getenv('TWITCH_CLIENT_SECRET')

base_path = "streams-tracker"
stream_files_path = "storage/streams"

redis_host = os.getenv('REDIS_HOST')
redis_password = os.getenv('REDIS_PASSWORD')
redis_port = os.getenv('REDIS_PORT')

secret_key = os.getenv('SECRET_KEY')

mongo_host = os.getenv('MONGO_HOST')
mongo_user = os.getenv('MONGO_USER')
mongo_password = os.getenv('MONGO_PASSWORD')

s3_url = os.getenv('S3_URL')
rw_access_key = os.getenv('RW_ACCESS_KEY')
rw_secret_key = os.getenv('RW_SECRET_KEY')
bucket_name = os.getenv('BUCKET_NAME')

