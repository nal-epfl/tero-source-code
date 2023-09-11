import cv2
import re

from frame_processing import pre_process_image, process_image
from config import extra_areas

def get_extra_areas(game_id):
    return extra_areas.get(game_id, {})


def parse_image_name(image):
    m = re.search(r"(?P<game_id>\d+)_(?P<date>\d\d\d\d-\d\d-\d\d-\d\d-\d\d-\d\d)_(?P<stream_id>\w+)_(?P<user_id>\w+)", image)
    if m:
        return {"game_id": m.group("game_id"), "date": m.group("date"), "stream_id": m.group("stream_id"), "user_id": m.group("user_id")}


def get_bw_image(img):
    try:
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    except Exception as e:
        return None

    return process_image(pre_process_image(img, gray))
    

def run_preprocessing(aoi, read_function, write_function, image):
    image_info = parse_image_name(image)
    img = read_function(image, image_info)

    if img is None:
        return None

    size = img.shape 
    areas = aoi.get(image_info["game_id"], {})

    if str(size[0]) in areas:
        candidate_areas = areas[str(size[0])]
        
        for area_idx in range(0, len(candidate_areas)):
            area = candidate_areas[area_idx]

            # Store "raw" tiny image
            ping_area = img[area["y1"]:area["y2"], area["x1"]:area["x2"]]
            ping_area_name = '{}_area{}.png'.format(image.split("/")[-1].split(".")[0], area_idx)
            write_function(ping_area, ping_area_name, "tiny")

            extra_area = get_extra_areas(image_info["game_id"])
            if extra_area:
                extra_area_img = img[extra_area["y1"]:extra_area["y2"], extra_area["x1"]:extra_area["x2"]]
                extra_area_name = '{}_extra.png'.format(image.split("/")[-1].split(".")[0])
                write_function(extra_area_img, extra_area_name, "tiny")
                
            erosion = get_bw_image(ping_area)
            if erosion is not None:
                bw_img_name = "{}_area{}_bw.png".format(image.split("/")[-1].split(".")[0], area_idx)
                write_function(erosion, bw_img_name, "bw")
           
    return image
    