import os
import re
import json
import sys
import timeit
import redis
import multiprocessing

import pytesseract
from pytesseract import Output
from config import number_cores_tesseract, redis_host, redis_port, redis_password, tiny_img_storage, tiny_to_process_path, tiny_results_storage, tesseract_config

queue_name = "tesseract_confirm"



def parse_image_name(image):
    m = re.search(r"(?P<game>\d+)_(?P<date>\d\d\d\d-\d\d-\d\d-\d\d-\d\d-\d\d)_(?P<stream_id>\w+)_(?P<user_id>\w+)_area(?P<area>\d+)", image)
    if m:
        return {"game_id": m.group("game"), "date": m.group("date"), "stream_id": m.group("stream_id"), "user_id": m.group("user_id"), "area_id": int(m.group("area"))}
    
    m = re.search(r"(?P<game>\d+)_(?P<date>\d\d\d\d-\d\d-\d\d-\d\d-\d\d-\d\d)_(?P<stream_id>\w+)_(?P<user_id>\w+)", image)
    if m:
        return {"game_id": m.group("game"), "date": m.group("date"), "stream_id": m.group("stream_id"), "user_id": m.group("user_id"), "area_id": 0}


def process_image(thumbnail):    
    file_info = parse_image_name(thumbnail)
    psm = tesseract_config.get(file_info["game_id"], 7)
    
    try:
        results = pytesseract.image_to_data(thumbnail, output_type=Output.DICT, config="--psm {}".format(psm))
    except pytesseract.pytesseract.TesseractError:
        try:
            results = pytesseract.image_to_data(thumbnail, output_type=Output.DICT)
        except Exception:
            return {}

    return {"image": thumbnail, "results": results}


if __name__ == "__main__":
    thumbnails = []

    cache = redis.Redis(host=redis_host, port=redis_port, password=redis_password)
    all_to_process = cache.spop(queue_name, count=1)

    if not all_to_process:
        sys.exit(0)
    
    for to_process in all_to_process:
        to_process = to_process.decode("utf-8")
    
        try:
            with open("{}/{}.txt".format(tiny_to_process_path, to_process), "r") as f:
                for l in f:
                    thumbnails.append("{}/{}/{}".format(tiny_img_storage, to_process, l.strip()))

            start_time = timeit.default_timer()

            pool = multiprocessing.Pool(number_cores_tesseract)
            results = pool.imap(func=process_image, iterable=thumbnails)

            with open("{}/{}_confirmation-pytesseract.json".format(tiny_results_storage, to_process), "a+") as out:
                for r in results:
                    if r:
                        out.write(json.dumps(r) + "\n")
                        
            with open('{}/{}_log-pytesseract.txt'.format(tiny_results_storage, to_process), 'a+') as f:
                f.write("Total images to process: {} \n".format(len(thumbnails)))
                f.write("Total processing time: {:.2f} sec \n\n".format(timeit.default_timer() - start_time))            
        except Exception:
            continue       