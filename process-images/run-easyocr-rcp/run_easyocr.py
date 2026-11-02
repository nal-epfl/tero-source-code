import chunk
import easyocr
import json
import numpy as np
import sys
import timeit
import redis
import PIL
import os
from tqdm import tqdm 

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

results_storage = "/scratch/image-processing/results"
to_process_storage = "/scratch/image-processing/to-process"
img_storage = "/scratch/image-processing/images-in-process"


PIL.Image.MAX_IMAGE_PIXELS = None


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

    batches_ready = set()
    for result in os.listdir(results_storage):
        if "log" in result and "easyocr" in result:
            batches_ready.add(result.split("_")[0])

    to_process = []
    for tp in os.listdir(to_process_storage):
        batch_name = tp.split(".")[0]
        
        if batch_name not in batches_ready:
            to_process.append(batch_name)
    
    if not to_process:
        sys.exit(0)

    to_process = to_process[0]
    eprint("Found to process: {}".format(to_process))

    already_processed = set()
    if os.path.isfile("{}/{}_matches-easyocr.json".format(results_storage, to_process)):
        with open("{}/{}_matches-easyocr.json".format(results_storage, to_process), "r") as f:
            for l in f:
                data = json.loads(l.strip())
                already_processed.add(data["image"])

    eprint("Already processed: {}".format(len(already_processed)))

    reader = easyocr.Reader(['en'], gpu=True)

    with open("{}/{}.txt".format(to_process_storage, to_process), "r") as f:
        for l in f:
            img_path = "{}/{}".format(img_storage, l.strip())
            if img_path not in already_processed:
                thumbnails.append(img_path)

    eprint("To process: {}".format(len(thumbnails)))

    start_time = timeit.default_timer()
    
    eprint("Starting process")
    with open("{}/{}_matches-easyocr.json".format(results_storage, to_process), "a+") as out:
        for t in tqdm(thumbnails):
            eprint(t)
            
            if not t:
                eprint("\t Skipping")
                continue
            try: 
                out.write(json.dumps({"image": t, "results": reader.readtext(t)}, cls=NpEncoder) + "\n")               
            except Exception as e:
                eprint(e)

    with open('{}/{}_log-easyocr.txt'.format(results_storage, to_process), 'a+') as f:
        f.write("Total images to process: {} \n".format(len(thumbnails)))
        f.write("Total processing time: {:.2f} sec \n\n".format(timeit.default_timer() - start_time))
