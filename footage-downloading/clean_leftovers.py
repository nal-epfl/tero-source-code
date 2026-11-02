import os
import re
from logger import get_logger

from config import base_frame_destination_path

def parse_file_name(img_name):
    m = re.search(r"(?P<game>\d+)_(?P<user_id>\w+)_(?P<stream_id>\w+)_(?P<date>\d+)-(?P<index>\d+)", img_name)
    if m:
        return {"game_id": m.group("game"), "date": m.group("date"), "stream_id": m.group("stream_id"), "user_id": m.group("user_id"), "idx": m.group("index")}


if __name__ == "__main__":
    logger = get_logger("clean-leftovers")

    logger.info("Checking directory...")

    deleted_counter = 0
    for file_name in os.listdir(base_frame_destination_path):
        image_info = parse_file_name(file_name)
        
        if image_info and int(image_info["idx"]) > 10:
            if os.path.isfile("{}/{}".format(base_frame_destination_path, file_name)):
                os.remove("{}/{}".format(base_frame_destination_path, file_name))
                deleted_counter += 1

    logger.info("Deleted {} images.".format(deleted_counter))