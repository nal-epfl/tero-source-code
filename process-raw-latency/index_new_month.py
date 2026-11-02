from pymongo import MongoClient
from datetime import datetime
from config import redis_host, redis_password, bucket_name, mongo_host, mongo_user, mongo_password, rw_access_key, rw_secret_key, s3_url
from logger import get_logger


if __name__ == "__main__":
    logger = get_logger("index_new_month")
    now = datetime.now()
    logger.info("Starting process...")

    mongo_client = MongoClient("mongodb://{}:27017/".format(mongo_host),
                                            username=mongo_user,
                                            password=mongo_password) 
    thumbnail_db = mongo_client.thumbnail_data
    footage_db = mongo_client.footage_data

    thumbnail_alternatives_db = mongo_client.thumbnail_alternatives
    footage_alternatives_db = mongo_client.footage_alternatives

    logger.info("Indexing 'thumbnails'")
    
    thumbnail_db["latency_{}_{:02d}".format(now.year, now.month)].create_index("user_id")
    thumbnail_db["latency_{}_{:02d}".format(now.year, now.month)].create_index("stream_id")
    thumbnail_db["latency_{}_{:02d}".format(now.year, now.month)].create_index("date")
    thumbnail_db["latency_{}_{:02d}".format(now.year, now.month)].create_index([("game_id", 1), ("user_id", 1), ("date", 1)])
    thumbnail_db["latency_{}_{:02d}".format(now.year, now.month)].create_index([("game_id", 1), ("user_id", 1), ("stream_id", 1), ("date", 1)])

    logger.info("Indexing 'footage'")

    footage_db["latency_{}_{:02d}".format(now.year, now.month)].create_index("user_id")
    footage_db["latency_{}_{:02d}".format(now.year, now.month)].create_index("stream_id")
    footage_db["latency_{}_{:02d}".format(now.year, now.month)].create_index("date")
    footage_db["latency_{}_{:02d}".format(now.year, now.month)].create_index([("game_id", 1), ("user_id", 1), ("date", 1)])
    footage_db["latency_{}_{:02d}".format(now.year, now.month)].create_index([("game_id", 1), ("user_id", 1), ("stream_id", 1), ("date", 1)])

    logger.info("Indexing 'thumbnail alternatives'")
    
    thumbnail_alternatives_db["alternatives_{}_{:02d}".format(now.year, now.month)].create_index("user_id")
    thumbnail_alternatives_db["alternatives_{}_{:02d}".format(now.year, now.month)].create_index("stream_id")
    thumbnail_alternatives_db["alternatives_{}_{:02d}".format(now.year, now.month)].create_index("date")
    thumbnail_alternatives_db["alternatives_{}_{:02d}".format(now.year, now.month)].create_index([("game_id", 1), ("user_id", 1), ("date", 1)])
    thumbnail_alternatives_db["alternatives_{}_{:02d}".format(now.year, now.month)].create_index([("game_id", 1), ("user_id", 1), ("stream_id", 1), ("date", 1)])

    logger.info("Indexing 'footage alternatives'")

    footage_alternatives_db["alternatives_{}_{:02d}".format(now.year, now.month)].create_index("user_id")
    footage_alternatives_db["alternatives_{}_{:02d}".format(now.year, now.month)].create_index("stream_id")
    footage_alternatives_db["alternatives_{}_{:02d}".format(now.year, now.month)].create_index("date")
    footage_alternatives_db["alternatives_{}_{:02d}".format(now.year, now.month)].create_index([("game_id", 1), ("user_id", 1), ("date", 1)])
    footage_alternatives_db["alternatives_{}_{:02d}".format(now.year, now.month)].create_index([("game_id", 1), ("user_id", 1), ("stream_id", 1), ("date", 1)])

    logger.info("Finished process...")