import sys
import json
import timeit
import os
import sys
import multiprocessing
from functools import partial

from common import run_preprocessing

from config import number_cores, base_path
from storage.s3_controller import S3StorageController
from logger import get_logger


if __name__ == '__main__':    
    idx = 0
    if len(sys.argv) > 1:
        idx = sys.argv[1]

    storage = S3StorageController(idx)
    logger = get_logger("pre_process_images")

    def read_function(x, metadata):
        return storage.read_image(x, metadata)


    def write_function(x, y, z):
        storage.store_image(x, y, z)

    if not os.path.isdir("logs"):
        os.makedirs("logs")
   
    with open('{}/data/areas_of_interest.json'.format(base_path), 'r') as f:
        areas = json.load(f)
    
    thumbnails = storage.get_images()
    
    if not thumbnails:
        logger.info("Not enough thumbnails to process. Stopping.")
        sys.exit(0)

    logger.info("Got {} thumbnails to process.".format(len(thumbnails)))
    
    pool = multiprocessing.Pool(number_cores)

    temp = partial(run_preprocessing, areas, read_function, write_function)
    results = pool.imap(func=temp, iterable=thumbnails)

    logger.info("Starting processing...")
    start_time = timeit.default_timer()

    to_delete = []
    
    for r in results:
        if r:
            to_delete.append(r)       

    logger.info("Finished processing. Total processing time: {:.2f} sec".format(timeit.default_timer() - start_time))            
    storage.finish(thumbnails, to_delete, logger)
