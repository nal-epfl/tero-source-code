import os
from dotenv import load_dotenv

load_dotenv()

batch_size = 5
processing_queues = ["easyocr", "paddleocr", "tesseract"]

redis_host = os.getenv('REDIS_HOST')
redis_password = os.getenv('REDIS_PASSWORD')
redis_port = os.getenv('REDIS_PORT')

img_storage = "image-processing/images-in-process"
results_storage = "image-processing/results"
to_process_storage = "image-processing/to-process"
base_path = "process-images"
long_term_storage = "image-processing/long-term-storage"

s3_url = os.getenv('S3_URL')
rw_access_key = os.getenv('RW_ACCESS_KEY')
rw_secret_key = os.getenv('RW_SECRET_KEY')
bucket_name = os.getenv('BUCKET_NAME')

number_cores_templates = 10
number_cores_tesseract = 5
max_simultaneous = 5

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