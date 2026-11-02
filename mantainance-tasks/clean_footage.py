import os
import re

from logger import get_logger

footage_storage_path = "/raid1/footage-storage"

def parse_image_name(image_name):
    m = re.search(r"(?P<game>\d+)_(?P<user_id>\w+)_(?P<stream_id>\w+)_(?P<date>\d+)-(?P<idx>\d+)", image_name)
    if m:
        return {"game_id": m.group("game"), "date": m.group("date"), "stream_id": m.group("stream_id"), "user_id": m.group("user_id"), "image_idx": int(m.group("idx"))}


if __name__ == "__main__":
    logger = get_logger("clean_footage_storage")

    logger.info("Starting process...")

    images_to_check = [image_name for image_name in os.listdir(footage_storage_path)]
    
    logger.info("Found {} images in path".format(len(images_to_check)))

    deleted = 0
    for image_name in images_to_check:
        image_info = parse_image_name(image_name)

        if image_info and image_info["image_idx"] > 10:
            if os.path.isfile("{}/{}".format(footage_storage_path, image_name)):
                os.remove("{}/{}".format(footage_storage_path, image_name))
                deleted += 1
    
    logger.info("Finished. Deleted: {}".format(deleted))
    
