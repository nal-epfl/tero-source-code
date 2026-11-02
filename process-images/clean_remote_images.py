import boto3
import redis

from dateutil import parser
from datetime import datetime

s3_url = "https://s3.epfl.ch"
rw_access_key = "GS7WRY539NV612VFE94M"
rw_secret_key = "RFYFSplaO7y+UoxSTTEHRPI+sLQNtMrKKWEyDRkR"
bucket_name = "12550-cfa21cf1091c1e92a45a913a9204c3d5"

redis_password = "AvBJUtd3Ab44fw4z7Cf3JGDuDGypc3wu"
redis_host = "10.90.38.19"
redis_port = 6379

cache = redis.Redis(host=redis_host, port=redis_port, db=0, password=redis_password)


client = boto3.client(
    's3',
    aws_access_key_id=rw_access_key,
    aws_secret_access_key=rw_secret_key,
    endpoint_url=s3_url
)

def gather_images(time_now):
    games = [
        "295590",
    "116088",
    "135305",
    "118849",
    "273195",
    "464426",
    "273486",
    "319965",
    "747108",
    "267128",
    "314852",
    "742409",
    "461764",
    "614266",
    "128974",
    "762836",
    "970338",
    "101342",
    "452439",
    "819896",
    "902129"
    ]

    images_in_queue = set()

    in_queue = cache.smembers("raw_images")
    for s in in_queue:
        images_in_queue.add(s.decode("utf8"))

    storage = open("/home/calvarez/process-images/in_remote/clean_images_remote-{}.json".format(time_now), "w+")

    for game in games:
        for i in [*range(0, 10), "a", "b", "c", "d", "e", "f"]:
            for j in [*range(0, 10), "a", "b", "c", "d", "e", "f"]:
                for k in [*range(0, 10), "a", "b", "c", "d", "e", "f"]:
                    if i == 2 or j == 2 or k == 2:
                        continue

                    prefix = "{}_{}{}{}".format(game, i, j, k)
                    ob = client.list_objects(Bucket=bucket_name, Prefix="raw/{}".format(prefix))

                    if "Contents" in ob:
                        for key in ob['Contents']:
                            if key["Key"] not in images_in_queue:
                                    storage.write("{}\n".format(key["Key"]))


from tqdm import tqdm

def insert_images(time_now):
    to_insert = set()

    with open("/home/calvarez/process-images/in_remote/clean_images_remote-{}.json".format(time_now), "r") as f:
        for l in tqdm(f):
            file_name = l.strip()
            to_insert.add(file_name)
    
    for ti in tqdm(to_insert):
        cache.sadd("raw_images", ti)


if __name__ == "__main__":
    time_now = datetime.now().timestamp()

    gather_images(time_now)
    # insert_images(time_now)