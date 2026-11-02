import chunk
import easyocr
import json
import numpy as np
import sys
import timeit
import PIL
import os

results_storage = "/scratch/tiny-image-processing/results"
to_process_storage = "/scratch/tiny-image-processing/to-process"
img_storage = "/scratch/tiny-image-processing/images-in-process"


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
    batches_ready = set()
    for result in os.listdir(results_storage):
        if "log" in result and "easyocr" in result:
            batches_ready.add(result.split("_")[0])

    all_to_process = []
    for tp in os.listdir(to_process_storage):
        batch_name = tp.split(".")[0]
        
        if batch_name not in batches_ready:
            all_to_process.append(batch_name)

    if not all_to_process:
        sys.exit(0)
       
    reader = easyocr.Reader(['en'], gpu=True)

    for to_process in all_to_process:
        thumbnails = []
        
        already_processed = set()
        if os.path.isfile("{}/{}_confirmation-easyocr.json".format(results_storage, to_process)):
            with open("{}/{}_confirmation-easyocr.json".format(results_storage, to_process), "r") as f:
                for l in f:
                    data = json.loads(l.strip())
                    already_processed.add(data["image"])
    
        with open("{}/{}.txt".format(to_process_storage, to_process), "r") as f:
            for l in f:
                img_path = "{}/{}/{}".format(img_storage, to_process, l.strip())
                if img_path not in already_processed:
                    thumbnails.append("{}/{}/{}".format(img_storage, to_process, l.strip()))

        start_time = timeit.default_timer()
        
        with open("{}/{}_confirmation-easyocr.json".format(results_storage, to_process), "a+") as out:
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

