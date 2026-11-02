import os
from dotenv import load_dotenv

load_dotenv()

redis_host = os.getenv('REDIS_HOST')
redis_password = os.getenv('REDIS_PASSWORD')
redis_port = os.getenv('REDIS_PORT')

mongo_host = os.getenv('MONGO_HOST')
mongo_port = os.getenv('MONGO_PORT')
mongo_user = os.getenv('MONGO_USER')
mongo_password = os.getenv('MONGO_PASSWORD')

base_path = "process-images-tiny"

tiny_img_path = "tiny-image-processing/images-in-process"
tiny_results_path = "tiny-image-processing/results"
tiny_to_process_path = "tiny-image-processing/to-process"

long_term_storage = "tiny-image-processing/long-term-storage"

s3_url = os.getenv('S3_URL')
rw_access_key = os.getenv('RW_ACCESS_KEY')
rw_secret_key = os.getenv('RW_SECRET_KEY')
bucket_name = os.getenv('BUCKET_NAME')

number_cores_tesseract = 5

tesseract_config = {
    "118849": 6,
    "116088": 6,
    "29595": 6,
    "513143": 6,
    "267128": 6,
    "511224": 6,
    "452439": 6,
    "101342": 6,
    "902129": 6   
}

jumphost = os.getenv('JUMPHOST')
jumphost_user = os.getenv('JUMPHOST_USER')
jumphost_password = os.getenv('JUMPHOST_PASSWORD')

remote_path = ""