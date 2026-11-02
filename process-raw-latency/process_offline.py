import re
import os
import json
import copy

from collections import Counter
from datetime import datetime
from parsers import *


def parse_json_name(json_name):
    m = re.search(r"matches-(?P<engine>\w+)", json_name)
    if m:
        return {"engine": m.group("engine")}


def test_one():
    all_data_keys = {}

    with open("matches-paddleocr.json", "r") as f:            
        lines = f.readlines()

    parser = parsers.get("paddleocr")        
    parser.parse_file(lines, all_data_keys)
    parser.post_process()         


def process_images(parsers, to_save):
    to_return = {}
    
    latency = {}
    values = {}
    has_mark = False

    for parser in parsers.values():
        value = parser.get_value(to_save)
        values[parser.name] = value
        latency[parser.name] = int(value.get("latency", "-1"))

        if value and parser.name != "templates":
            has_mark = has_mark or value.get("has_mark", True)
    
    count = Counter([x for x in latency.values() if int(x) >= 0]).most_common()
    if not count:
        return {}
    
    value = count[0][0]
    c = count[0][1]

    if value < 0:
        return {}
    
    if c > len(latency.keys()) / 2:
        if "latency" not in to_save and has_mark:
            to_save["latency"] = str(value)
            to_save["values"] = values
                                                                            
            to_return["info_to_store"] = to_save
                        
    if c == len(latency.keys()) - 1 and len(count) > 1:
        if has_mark: 
            partial_value = copy.deepcopy(to_save)
            if "latency" in partial_value:
                partial_value.pop("latency")

            partial_value["values"] = values
            to_return["alternative_values"] = partial_value
                        
    if c == int(len(latency.keys()) / 2):
        to_save["real_latency"] = values
        to_return["to_confirm"] = to_save
        
    return to_return


def test_all():
    img_type = "bw"

    path = "/home/calvarez/Projects/Tero-Core/process-raw-latency/test"
    results = []

    for file_name in os.listdir(path):
        results.append(os.path.join(path, file_name))

    to_postprocess_metadata = [{"results": results}]

    for idx, metadata in enumerate(to_postprocess_metadata):
        parsers = {
            "easyocr": EasyOCRParser(img_type),
            "paddleocr": PaddleOCRParser(img_type),
            "pytesseract": PytesseractParser(img_type)
        }

        all_data_keys = {}
        all_token_keys = {}

        batch_name = None

        for result in metadata.get("results", []):
            json_info = parse_json_name(result)
            batch_name = "test-both"

            with open(result, "r") as f:            
                lines = [x for x in f.readlines()]
                            
            parser = parsers.get(json_info["engine"])        
            parser.parse_file(lines, all_data_keys, all_token_keys)

        for parser in parsers.values():
            parser.post_process()      

        information_to_save = {}
        to_confirm = []
        
        for game_id, users in all_data_keys.items():
            information_to_save[game_id] = []

            for user_id, streams in users.items():
                for stream_id, dates in streams.items():
                    for date in dates:
                        to_save = {"date": date, "game_id":  game_id, "user_id": user_id, "stream_id": stream_id}
                        latency = {}
                        values = {}
                        has_mark = False

                        for parser in parsers.values():
                            value = parser.get_value(to_save)
                            values[parser.name] = value
                            latency[parser.name] = int(value.get("latency", "-1")) if '.' not in value.get("latency", "-1") else float(value.get("latency", "-1"))

                            if value and parser.name != "templates":
                                has_mark = has_mark or value.get("has_mark", True)
                        
                        count = Counter(list(latency.values())).most_common(1)
                        value = count[0][0]
                        c = count[0][1]

                        if value < 0:
                            continue
                        
                        real_latency = None
                        if c > len(latency.keys()) / 2:
                            real_latency = value
                        
                        elif c == int(len(latency.keys()) / 2):
                            to_save["real_latency"] = values
                            to_confirm.append(to_save)

                        if real_latency and has_mark:
                            if "latency" not in to_save:
                                to_save["latency"] = str(real_latency)
                                to_save["values"] = values
                                                            
                                information_to_save[game_id].append(to_save)
        
        info_to_store = {}
        alternative_values = {}

        to_confirm = []
        
        for game_id, tokens in all_token_keys.items():
            info_to_store[game_id] = []
            alternative_values[game_id] = []

            for token, indexes in tokens.items():
                for idx in indexes:
                    to_save = {"game_id":  game_id, "token": token, "idx": idx}
                    processed = process_images(parsers, to_save)

                    if "info_to_store" in processed:
                        info_to_store[game_id].append(processed["info_to_store"])

                    if "alternative_values" in processed:
                        alternative_values[game_id].append(processed["alternative_values"])

                    if "to_confirm" in processed:
                        to_confirm.append(processed["to_confirm"])
                    
                    if "info_to_store" not in processed and "to_confirm" not in processed:
                        info_to_store[game_id].append(to_save)

        print(info_to_store)

        useful_per_game = {}
                
        for game_id, values in information_to_save.items():
            useful_per_game[game_id] = len(values)

        # Once you are finally done, you need to save the metadata
        metadata_line = {"batch_name": batch_name, **metadata, "images_stats": {"total": parsers.get("easyocr").report_processed(), "useful": useful_per_game}}
        
        results_json_name = "{}-results.json".format(batch_name)
        with open(results_json_name, "w+") as f:
            f.write(json.dumps(metadata_line) + "\n")
            
            for d in [information_to_save, info_to_store]:
                for game_id, values in d.items():
                    for value in values:
                        f.write(json.dumps(value) + "\n")

        if to_confirm:
                to_confirm_json_name = "{}-to_confirm.json".format(batch_name)  
                with open(to_confirm_json_name, "w+") as f:
                    for tc in to_confirm:
                        f.write("{}\n".format(json.dumps(tc)))


test_all()