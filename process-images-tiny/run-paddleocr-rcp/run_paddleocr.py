import sys
import os
import cv2
import timeit
import json
import numpy as np

from paddleocr import PaddleOCR

results_storage = "/scratch/tiny-image-processing/results"
to_process_storage = "/scratch/tiny-image-processing/to-process"
img_storage = "/scratch/tiny-image-processing/images-in-process"



class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)



def format_results(results):
    formatted_results = []

    for results_seq in results:
        boxes = results_seq.get("res", {}).get("rec_polys", [])
        scores = results_seq.get("res", {}).get("rec_scores", [])

        for idx, text in enumerate(results_seq.get("res", {}).get("rec_texts", [])):
            if idx < len(boxes) and idx < len(scores):
                corresponding_box = boxes[idx]
                corresponding_score = scores[idx]

                formatted_results.append([[corresponding_box, [text, corresponding_score]]])

    return formatted_results



if __name__ == "__main__":
    batches_ready = set()
    for result in os.listdir(results_storage):
        if "log" in result and "paddleocr" in result:
            batches_ready.add(result.split("_")[0])

    all_to_process = []
    for tp in os.listdir(to_process_storage):
        batch_name = tp.split(".")[0]
        
        if batch_name not in batches_ready:
            all_to_process.append(batch_name)

    if not all_to_process:
        sys.exit(0)
       
    ocr = PaddleOCR(
        use_doc_orientation_classify=False, # Disables document orientation classification model via this parameter
        use_doc_unwarping=False, # Disables text image rectification model via this parameter
        use_textline_orientation=False, # Disables text line orientation classification model via this parameter
        enable_mkldnn=False, 
        ocr_version="PP-OCRv4"
    )

    for to_process in all_to_process:
        thumbnails = []
        
        already_processed = set()
        if os.path.isfile("{}/{}_confirmation-paddleocr.json".format(results_storage, to_process)):
            with open("{}/{}_confirmation-paddleocr.json".format(results_storage, to_process), "r") as f:
                for l in f:
                    data = json.loads(l.strip())
                    already_processed.add(data["image"])
    
        with open("{}/{}.txt".format(to_process_storage, to_process), "r") as f:
            for l in f:
                img_path = "{}/{}/{}".format(img_storage, to_process, l.strip())
                if img_path not in already_processed:
                    thumbnails.append("{}/{}/{}".format(img_storage, to_process, l.strip()))

        start_time = timeit.default_timer()
        
        with open("{}/{}_confirmation-paddleocr.json".format(results_storage, to_process), "a+") as out:
            for t in thumbnails:
                if not t:
                    continue

                try: 
                    result = ocr.predict(t)

                    out.write(json.dumps({"image": t, 
                                          "results": format_results([l.json for l in result])}, cls=NpEncoder) + "\n")            
                except Exception:
                    pass

        with open('{}/{}_log-paddleocr.txt'.format(results_storage, to_process), 'a+') as f:
            f.write("Total images to process: {} \n".format(len(thumbnails)))
            f.write("Total processing time: {:.2f} sec \n\n".format(timeit.default_timer() - start_time))