import redis
import re

from logger import get_logger
from pymongo import MongoClient
from config import mongo_host, mongo_user, mongo_password, redis_host, redis_port, redis_password


if __name__ == "__main__":
    logger = get_logger("add_stream_files")

    logger.info("Starting process...")

    mongo_client = MongoClient('mongodb://{}:27017/'.format(mongo_host), username=mongo_user, password=mongo_password)
    storage = redis.Redis(host=redis_host, port=redis_port, password=redis_password)

    new_stream_files = storage.spop("new_stream_files", count=storage.scard("new_stream_files"))
    logger.info("Found to process: {}".format(len(new_stream_files)))

    for x in new_stream_files:
        m = re.search(r"(?P<date>\d\d\d\d-\d\d-\d\d-\d\d-\d\d)", x.decode("utf8"))
        if m:
            file_date = m.group("date")
            if mongo_client.stream_files.available.count_documents({"date": file_date}, limit=1) == 0:
                mongo_client.stream_files.available.insert_one({"date": file_date, "file_path": "/media/data/storage/streams/{}".format(x)})

    logger.info("Finished process...")