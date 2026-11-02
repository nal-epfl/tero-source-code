from pymongo import MongoClient
from datetime import timedelta, datetime
from config import mongo_host, mongo_user, mongo_password

from logger import get_logger


if __name__ == "__main__":
    logger = get_logger("streams_weekly_cleanup")

    logger.info("Starting process...")

    mongo_client = MongoClient("mongodb://{}:27017/".format(mongo_host),
                                username=mongo_user,
                                password=mongo_password) 

    three_weeks_ago = datetime.now() - timedelta(days=21)
    to_remove = mongo_client.thumbnails.current_streams.delete_many({"starting_time": {"$lt": three_weeks_ago.timestamp()}})

    logger.info("Deleted {} streams. Finishing process...".format(to_remove.deleted_count))
