import json
import boto3
import re
import hashlib
import hmac

from datetime import datetime
from collections import Counter
from parsers import *

import copy
from config import rw_access_key, rw_secret_key, s3_url, bucket_name, secret_key
from logger import get_logger

from db.online_controller import OnlineController


def parse_json_name(json_name):
    m = re.search(r"(?P<date>\d\d\d\d-\d\d-\d\d-\d\d-\d\d-\d\d)_matches-(?P<engine>\w+).json", json_name)
    if m:
        return {"date": m.group("date"), "engine": m.group("engine")}
    
    m = re.search(r"(?P<date>\d\d\d\d-\d\d-\d\d-\d\d-\d\d)_matches-(?P<engine>\w+).json", json_name)
    if m:
        return {"date": m.group("date"), "engine": m.group("engine")}



class RawLatencyProcessor:
    def __init__(self):
        self.logger = get_logger("process_raw_latency")
              
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=rw_access_key,
            aws_secret_access_key=rw_secret_key,
            endpoint_url=s3_url
        )

        self.controller = OnlineController(self.logger)


    def process_images(self, parsers, to_save):
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
                
                if self.controller.store_information(to_save):
                    to_return["info_to_store"] = to_save
                            
        if c == len(latency.keys()) - 1 and len(count) > 1:
            if has_mark: 
                partial_value = copy.deepcopy(to_save)
                if "latency" in partial_value:
                    partial_value.pop("latency")

                partial_value["values"] = values

                if self.controller.store_alternative_values(partial_value):   
                    to_return["alternative_values"] = partial_value
                            
        if c == int(len(latency.keys()) / 2):
            to_save["real_latency"] = values
            to_return["to_confirm"] = to_save
            
        return to_return


    def process_twitch_images(self, keys, parsers):
        info_to_store = {}
        alternative_values = {}

        to_confirm = []

        for game_id, users in keys.items():
            info_to_store[game_id] = []
            alternative_values[game_id] = []

            for user_id, streams in users.items():
                for stream_id, dates in streams.items():
                    try:
                        int(user_id)

                        user_id = hmac.new(secret_key.encode("utf-8"), user_id.encode("utf-8"), hashlib.sha1).hexdigest()
                        stream_id = hmac.new(secret_key.encode("utf-8"), stream_id.encode("utf-8"), hashlib.sha1).hexdigest()
                    except Exception:
                        pass
                    
                    for date in dates:
                        to_save = {"date": date, "game_id":  game_id, "user_id": user_id, "stream_id": stream_id}
                        processed = self.process_images(parsers, to_save)

                        if "info_to_store" in processed:
                            info_to_store[game_id].append(processed["info_to_store"])

                        if "alternative_values" in processed:
                            alternative_values[game_id].append(processed["alternative_values"])

                        if "to_confirm" in processed:
                            to_confirm.append(processed["to_confirm"])

        return info_to_store, alternative_values, to_confirm


    def process_token_images(self, keys, parsers):
        info_to_store = {}
        alternative_values = {}

        to_confirm = []
        
        for game_id, tokens in keys.items():
            info_to_store[game_id] = []
            alternative_values[game_id] = []

            for token, indexes in tokens.items():
                for idx in indexes:
                    to_save = {"game_id":  game_id, "token": token, "idx": idx}
                    processed = self.process_images(parsers, to_save)

                    if "info_to_store" in processed:
                        info_to_store[game_id].append(processed["info_to_store"])

                    if "alternative_values" in processed:
                        alternative_values[game_id].append(processed["alternative_values"])

                    if "to_confirm" in processed:
                        to_confirm.append(processed["to_confirm"])
                    
                    if "info_to_store" not in processed and "to_confirm" not in processed:
                        self.controller.store_information(to_save, always_insert=True)
                        info_to_store[game_id].append(to_save)

        return info_to_store, alternative_values, to_confirm


    def run(self):
        to_postprocess_metadata = self.controller.get_to_process()       

        if to_postprocess_metadata:
            self.logger.info("Got from redis: {}".format(to_postprocess_metadata))

        for idx, metadata in enumerate(to_postprocess_metadata):
            self.logger.info("Processing entry {}/{} entries to postprocess".format(idx+1, len(to_postprocess_metadata)))

            parsers = {
               "easyocr": EasyOCRParser(),
                "paddleocr": PaddleOCRParser(),
                "pytesseract": PytesseractParser()
            }   

            all_data_keys = {}
            all_token_keys = {}

            batch_name = None

            for result in metadata.get("results", []):
                json_info = parse_json_name(result)
                
                if json_info["engine"] == "templates":
                    continue

                self.logger.info("Parsing {}".format(result))

                batch_name = json_info["date"]

                content_object = self.s3_client.get_object(Bucket=bucket_name, Key=result)
                file_content = content_object['Body'].read().decode('utf-8')
                lines = file_content.split("\n")
                
                parser = parsers.get(json_info["engine"])        
                parser.parse_file(lines, all_data_keys, all_token_keys)

            for parser in parsers.values():
                parser.post_process()      

            self.logger.info("Finished parsing, starting comparison process")

            twitch_info_to_store, twitch_alternatives, twitch_to_confirm = self.process_twitch_images(all_data_keys, parsers)
            token_info_to_store, token_alternatives, token_to_confirm = self.process_token_images(all_token_keys, parsers)
                        
            useful_per_game = {}
                    
            for game_id, values in twitch_info_to_store.items():
                useful_per_game[game_id] = len(values)

            # Once you are finally done, you need to save the metadata
            metadata_line = {"batch_name": batch_name, **metadata, "images_stats": {"total": parsers.get("easyocr").report_processed(), "useful": useful_per_game}}

            if token_info_to_store:
                token_useful_per_game = {}
                    
                for game_id, values in token_info_to_store.items():
                    token_useful_per_game[game_id] = len(values)

                metadata_line["tokens"] = {"total": parsers.get("easyocr").report_token_processed(), "useful": token_useful_per_game}

            self.logger.info("Starting to compile all the results into a json")
            results_json_name = "{}-results.json".format(batch_name)
            
            users_presence = {}
            with open(results_json_name, "w+") as f:
                f.write(json.dumps(metadata_line) + "\n")
                
                for tag, d in {"thumbnails": twitch_info_to_store, "footage": token_info_to_store}.items():
                    for game_id, values in d.items():
                        for value in values:
                            if "_id" in value:
                                value.pop("_id")
                            f.write(json.dumps(value) + "\n")

                            if tag == "thumbnails":
                                date = datetime.fromtimestamp(int(value["date"]))
                                month = "latency_{}_{:02d}".format(date.year, date.month)

                                if value["user_id"] not in users_presence:
                                    users_presence[value["user_id"]] = set()

                                users_presence[value["user_id"]].add(month)

            self.controller.insert_user_presence(users_presence, data_type="latency")

            alternatives_presence = {}
            alternatives_json_name = "{}-alternative-values.json".format(batch_name)
            with open(alternatives_json_name, "w+") as f:
                for tag, d in {"thumbnails": twitch_alternatives, "footage": token_alternatives}.items():
                    for game_id, values in d.items():
                        for value in values:
                            if "_id" in value:
                                value.pop("_id")
                            f.write(json.dumps(value) + "\n")
                            
                            if tag == "thumbnails":
                                date = datetime.fromtimestamp(int(value["date"]))
                                month = "alternatives_{}_{:02d}".format(date.year, date.month)

                                if value["user_id"] not in alternatives_presence:
                                    alternatives_presence[value["user_id"]] = set()

                                alternatives_presence[value["user_id"]].add(month)

            self.controller.insert_user_presence(alternatives_presence, data_type="alternatives")

            jsons_to_upload = [results_json_name, alternatives_json_name]

            if [*twitch_to_confirm, *token_to_confirm]:
                to_confirm_json_name = "{}-to_confirm.json".format(batch_name)  
                with open(to_confirm_json_name, "w+") as f:
                    for tc in [*twitch_to_confirm, *token_to_confirm]:
                        f.write("{}\n".format(json.dumps(tc)))

                jsons_to_upload.append(to_confirm_json_name)
                self.controller.store_to_confirm(batch_name)

            self.logger.info("Inserting metadata in Mongo")
            
            if [*twitch_info_to_store, *token_info_to_store, *twitch_alternatives, *token_alternatives]:
                self.controller.store_metadata(metadata_line)
                self.controller.store_jsons(jsons_to_upload)
            
                self.controller.clean_up(metadata_line)

            self.logger.info("Finished processing...")
    
    
if __name__ == "__main__":
    processor = RawLatencyProcessor()
    processor.run()
