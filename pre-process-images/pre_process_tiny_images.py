import os
import json
import multiprocessing
import sys
import timeit
import cv2

from common import get_bw_image
from datetime import datetime
from config import batch_size, number_cores, index_storage_path
from storage.local_storage_controller import LocalStorageController

from logger import get_logger
from time import sleep
from tqdm import tqdm

game_id = ""

logger = get_logger("tiny_images")


def process_tiny_image(image):
    img = cv2.imread(image)
    erosion = get_bw_image(img)

    if erosion is not None:
        if game_id:
            bw_img_name = "{}_{}_bw.png".format(game_id, image.split("/")[-1].split(".")[0])
        else:
            bw_img_name = "{}_bw.png".format(image.split("/")[-1].split(".")[0])
        
        storage.store_image(erosion, bw_img_name, "bw")       
        
        return image
    

if __name__ == '__main__':
    base_path = ""
    to_skip = set()

    with open("already_processed.txt", "r") as f:
        for l in f:
            to_skip.add(l.strip())

    for path in tqdm(os.listdir(base_path)):
        if path in to_skip:
            continue

        # imgs_path = os.path.join(base_path, path, "imgs")
        imgs_path = os.path.join(base_path, path)

        thumbnails = [os.path.join(imgs_path, x) for x in os.listdir(imgs_path)]
        thumbnails_chunks = [thumbnails[i:i + batch_size] for i in range(0, len(thumbnails), batch_size)]
        
        storage = LocalStorageController(imgs_path, game_id)

        for idx, chunk in enumerate(thumbnails_chunks):
            logger.info("Starting processing of chunk {} of path {}".format(idx, path))

            pool = multiprocessing.Pool(number_cores)
            
            start_time = timeit.default_timer()
            now = datetime.now().strftime("%Y-%m-%d-%H-%M")

            results = pool.imap(func=process_tiny_image, iterable=chunk)

            to_delete = []

            for r in results:
                if r is not None:
                    to_delete.append(r)    
            
            logger.info("Finished processing. Total processing time: {:.2f} sec".format(timeit.default_timer() - start_time))            
            # now = storage.finish(thumbnails, to_delete, logger)
            
            # with open("{}/{}_index.txt".format(index_storage_path, now), "w+") as f:
            #     for t in to_delete:
            #         f.write("{}\n".format(t.split("/")[-1]))
            
            storage.create_local_tmp()
        
        logger.info("Finished processing path {}".format(path))