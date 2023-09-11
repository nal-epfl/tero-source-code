import chunk
import easyocr
import json
import numpy as np
import sys
import timeit
import redis
import PIL


from config import redis_host, redis_port, redis_password, results_storage, to_process_storage, img_storage

PIL.Image.MAX_IMAGE_PIXELS = None
queue_name = "easyocr"


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
       
    reader = easyocr.Reader(['en'])

    to_process = to_process[0].decode("utf-8")

    with open("{}/{}.txt".format(to_process_storage, to_process), "r") as f:
        for l in f:
            thumbnails.append("{}/{}".format(img_storage, l.strip()))

    start_time = timeit.default_timer()
    
    with open("{}/{}_matches-easyocr.json".format(results_storage, to_process), "a+") as out:
        for t in thumbnails:
            if not t:
                continue

            try: 
                out.write(json.dumps({"image": t, "results": reader.readtext(t, batch_size=4)}, cls=NpEncoder) + "\n")               
            except Exception:
                pass

    with open('{}/{}_log-easyocr.txt'.format(results_storage, to_process), 'a+') as f:
        f.write("Total images to process: {} \n".format(len(thumbnails)))
        f.write("Total processing time: {:.2f} sec \n\n".format(timeit.default_timer() - start_time))
