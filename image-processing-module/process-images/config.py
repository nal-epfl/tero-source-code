batch_size = 2
processing_queues = ["easyocr", "paddleocr", "tesseract"]

redis_host = "127.0.0.1"
redis_port = 0
redis_password = ""

img_storage = ""
results_storage = ""
to_process_storage = ""

base_path = ""
long_term_storage = ""

s3_url = ""
rw_access_key = ""
rw_secret_key = ""
bucket_name = ""

number_cores_tesseract = 1
max_simultaneous = 1

tesseract_config = {
    "game1": 6,
    "game2": 6,
}