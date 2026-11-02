from email.mime import base
import json
import timeit
import os
import sys
import multiprocessing
from functools import partial

from common import run_preprocessing

from config import number_cores, base_path, batch_size
from storage.local_storage_controller import LocalStorageController
from logger import get_logger

logger = get_logger("pre_process_images_batch")

game_id = ""
path = "/media/ripe3/ui-test-images"

storage = LocalStorageController(path)

def read_function(x, metadata):
    return storage.read_image(x, metadata)


def write_function(x, y, z):
    storage.store_image(x, y, z)


if __name__ == '__main__':    
    if not os.path.isdir("logs"):
        os.makedirs("logs")
   
    with open('{}/data/areas_of_interest.json'.format(base_path), 'r') as f:
        areas = json.load(f)
       
    thumbnails = []
    for f in os.listdir(path):
        if "121617" in f:
            thumbnails.append(os.path.join(path, f)) 

    # for x in os.listdir("{}/{}".format(storage_path, game_id)):
    #     thumbnails.append("{}_{}".format(game_id, x))

    thumbnails_chunks = [thumbnails[i:i + batch_size] for i in range(0, len(thumbnails), batch_size)]

    for chunk in thumbnails_chunks:
        logger.info("Got {} thumbnails to process.".format(len(chunk)))
        
        pool = multiprocessing.Pool(number_cores)

        temp = partial(run_preprocessing, areas, read_function, write_function)
        results = pool.imap(func=temp, iterable=chunk)

        logger.info("Starting processing...")
        start_time = timeit.default_timer()

        to_delete = []
        
        for r in results:
            if r is not None:
                to_delete.append(r)       

        logger.info("Finished processing. Total processing time: {:.2f} sec".format(timeit.default_timer() - start_time))            
        # storage.finish(chunk, to_delete, logger)
        # storage.create_local_tmp()
        