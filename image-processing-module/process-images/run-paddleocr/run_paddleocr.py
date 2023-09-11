import sys
import os
import cv2
import redis
import timeit
import json
import numpy as np

from paddleocr import PaddleOCR
from config import redis_host, redis_port, redis_password, results_storage, to_process_storage, img_storage

queue_name = "paddleocr"


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)


if __name__ == "__main__":
    thumbnails = []

    cache = redis.Redis(host=redis_host, port=redis_port, password=redis_password)
    to_process = cache.spop(queue_name, count=1)

    if not to_process:
        sys.exit(0)
    
    to_process = to_process[0].decode("utf-8")

    with open("{}/{}.txt".format(to_process_storage, to_process), "r") as f:
        for l in f:
            thumbnails.append("{}/{}".format(img_storage, l.strip()))

    ocr = PaddleOCR(use_angle_cls=True, use_gpu=True, lang='en')

    start_time = timeit.default_timer()

    with open("{}/{}_matches-paddleocr.json".format(results_storage, to_process), "a+") as out:
        for t in thumbnails:
            try:
                result = ocr.ocr(t, cls=True)
            
                out.write(json.dumps({"image": t, 
                                    "results": [l for l in result]}, cls=NpEncoder) + "\n")
            except Exception:
                pass

    with open('{}/{}_log-paddleocr.txt'.format(results_storage, to_process), 'a+') as f:
        f.write("Total images to process: {} \n".format(len(thumbnails)))
        f.write("Total processing time: {:.2f} sec \n\n".format(timeit.default_timer() - start_time))            
