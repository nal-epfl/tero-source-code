import os
from dotenv import load_dotenv

load_dotenv()

games_to_probe = ['21779', '513181', '29595', '513143', '510218', '512710', '1614555304', '490100', '213930085', 
                  '32502', '27546', '460630', '515025', '66366', '506416', '490377', '500188', '209738946', '1264310518', "1448964995"]

users_batch_size = 10000

redis_host = os.getenv('REDIS_HOST')
redis_password = os.getenv('REDIS_PASSWORD')
redis_port = os.getenv('REDIS_PORT')

base_path = "images-downloader"

width = 1920
height = 1080

offline_expire = 3 * 30
finished_expire = 10 * 30
default_to_sleep = 5

twitch_api_id = os.getenv('TWITCH_API_ID')
twitch_client_secret = os.getenv('TWITCH_CLIENT_SECRET')

rate_limit_safety_factor = 0.8

process_check_frequency = 30
max_queue_size = 300
min_queue_size = 100

downloader_fetch_count = 50

# Local storage
path_to_storage = "/data/backup"

# S3 information
s3_url = os.getenv('S3_URL')
rw_access_key = os.getenv('RW_ACCESS_KEY')
rw_secret_key = os.getenv('RW_SECRET_KEY')
bucket_name = os.getenv('BUCKET_NAME')
raw_images_path = "raw"

secret_key = os.getenv('SECRET_KEY')

mongo_host = os.getenv('MONGO_HOST')
mongo_user = os.getenv('MONGO_USER')
mongo_password = os.getenv('MONGO_PASSWORD')

CLIENT_ID = os.getenv('CLIENT_ID')
