import sys
import os
import cv2
import timeit
import json
import numpy as np

from paddleocr import PaddleOCR


results_storage = "/scratch/image-processing/results"
to_process_storage = "/scratch/image-processing/to-process"
img_storage = "/scratch/image-processing/images-in-process"



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

    to_process = []
    for tp in os.listdir(to_process_storage):
        to_process.append(tp.split(".")[0])
    
    if not to_process:
        sys.exit(0)
    
    to_process = to_process[0]

    if not os.path.isdir("{}/{}".format(results_storage, to_process)):
        os.makedirs("{}/{}".format(results_storage, to_process))

    already_processed = set()
    for file_name in os.listdir("{}/{}".format(results_storage, to_process)):
        already_processed.add(file_name.replace("_res.json", ".png"))

    with open("{}/{}.txt".format(to_process_storage, to_process), "r") as f:
        for l in f:
            img_path = "{}/{}".format(img_storage, l.strip())
            if img_path not in already_processed:
                thumbnails.append(img_path)

    ocr = PaddleOCR(
        use_doc_orientation_classify=False, # Disables document orientation classification model via this parameter
        use_doc_unwarping=False, # Disables text image rectification model via this parameter
        use_textline_orientation=False, # Disables text line orientation classification model via this parameter
        enable_mkldnn=False, 
    )

    start_time = timeit.default_timer()
    for t in thumbnails:
        try:
            result = ocr.predict(t)

            for res in result:  
                res.save_to_json(save_path="{}/{}".format(results_storage, to_process)) 
            
        except Exception as e:
            print(e)

    with open('{}/{}_log-paddleocr.txt'.format(results_storage, to_process), 'a+') as f:
        f.write("Total images to process: {} \n".format(len(thumbnails)))
        f.write("Total processing time: {:.2f} sec \n\n".format(timeit.default_timer() - start_time))    

    # if os.path.isdir("{}/{}.txt".format(to_process_storage, to_process)):
    #     os.remove("{}/{}.txt".format(to_process_storage, to_process))        
