import sys
import os
import cv2
import redis
import timeit
import json
import numpy as np

from paddleocr import PaddleOCR
from config import redis_host, redis_password, results_storage, to_process_storage, img_storage

queue_name = "paddleocr_confirm"


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

    cache = redis.Redis(host=redis_host, port=6379, password=redis_password) 
    to_process = cache.scard(queue_name)
    if to_process < 1:
        sys.exit(0)
    
    all_to_process = cache.spop(queue_name, count=10)
    
    for to_process in all_to_process:
        to_process = to_process.decode("utf-8")

        try:
            with open("{}/{}.txt".format(to_process_storage, to_process), "r") as f:
                for l in f:
                    thumbnails.append("{}/{}/{}".format(img_storage, to_process, l.strip()))

            ocr = PaddleOCR(use_angle_cls=True, use_gpu=True, lang='en')

            start_time = timeit.default_timer()

            with open("{}/{}_confirmation-paddleocr.json".format(results_storage, to_process), "a+") as out:
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
        except Exception:
            continue   