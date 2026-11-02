import redis
import os

from config import base_path, bucket_name, index_storage_path, long_term_storage, redis_host, redis_password

redis_cache = redis.Redis(host=redis_host, port=6379, password=redis_password)

path = "/raid1/storage/images-index"

for x in os.listdir(path):
    if "2026-03-12" not in x:
        continue
    
    with open("{}/{}".format(path, x), "r") as f:        
        for l in f:
            img = l.strip()
            if "extra" in img:
                continue

            redis_cache.sadd("raw_images_to_delete", "raw/{}.png".format(img.split("_area")[0]))
            