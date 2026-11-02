import re
import os
import cv2
import json
import multiprocessing

from functools import partial
from config import base_path

path_to_store = ""
batch_size = 100
number_cores = 6
game_id = ""


def parse_image_name(image):
    m = re.search(r"(?P<date>\d\d\d\d-\d\d-\d\d-\d\d-\d\d-\d\d)_(?P<stream_id>\w+)_(?P<user_id>\w+)", image)
    if m:
        return {"date": m.group("date"), "stream_id": m.group("stream_id"), "user_id": m.group("user_id")}        
        


def read_function(image):
    try:            
        img = cv2.imread(image)
        return img
    except Exception as e:
        return None       


def write_function(image, image_name):
    path = "{}/{}".format(path_to_store, image_name)
    cv2.imwrite(path, image)


def process(aoi, image):
    img = read_function(image)

    if img is None:
        return None

    size = img.shape 
    areas = aoi.get(game_id, {})

    if str(size[0]) in areas:
        candidate_areas = areas[str(size[0])]
        
        for area_idx in range(0, len(candidate_areas)):
            area = candidate_areas[area_idx]

            # Store "raw" tiny image
            ping_area = img[area["y1"]:area["y2"], area["x1"]:area["x2"]]
            ping_area_name = '{}_area{}.png'.format(image.split("/")[-1].split(".")[0], area_idx)
            write_function(ping_area, ping_area_name)



if __name__ == '__main__':    
    storage_path = ""

    with open('{}/data/areas_of_interest.json'.format(base_path), 'r') as f:
        areas = json.load(f)
    
    thumbnails = []
    for x in os.listdir(storage_path):
        thumbnails.append("{}/{}".format(storage_path, x))

    thumbnails_chunks = [thumbnails[i:i + batch_size] for i in range(0, len(thumbnails), batch_size)]

    for chunk in thumbnails_chunks:       
        pool = multiprocessing.Pool(number_cores)

        temp = partial(process, areas)
        results = pool.imap(func=temp, iterable=chunk)

        to_delete = []
        
        for r in results:
            if r is not None:
                to_delete.append(r)       
