import os
from dotenv import load_dotenv

load_dotenv()

batch_size = 10000

base_path = "pre-process-images"
index_storage_path = "storage/images-index"
long_term_storage = "image-processing/zip-storage"

storage_path = ""

s3_url = os.getenv('S3_URL')
rw_access_key = os.getenv('RW_ACCESS_KEY')
rw_secret_key = os.getenv('RW_SECRET_KEY')
bucket_name = os.getenv('BUCKET_NAME')

redis_host = os.getenv('REDIS_HOST')
redis_password = os.getenv('REDIS_PASSWORD')
redis_port = os.getenv('REDIS_PORT')

mongo_host = os.getenv('MONGO_HOST')
mongo_user = os.getenv('MONGO_USER')
mongo_password = os.getenv('MONGO_PASSWORD')

number_cores = 2
increase_batch_threshold = 2

paths_to_check = ["pre-process-images/logs",
                  "pre-process-images",
                  "/",
                  "image-processing/zip-storage"]

