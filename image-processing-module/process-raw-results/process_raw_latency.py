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

from db.offline_controller import OfflineController


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

        self.controller = OfflineController(self.logger)


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
                parser.parse_file(lines, all_data_keys)

            for parser in parsers.values():
                parser.post_process()      

            info_to_store = {}
            alternative_values = {}

            to_confirm = []
            stream_ends = {}

            self.logger.info("Finished parsing, starting comparison process")

            for game_id, users in all_data_keys.items():
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

                        if stream_id not in stream_ends:
                            stream_ends[stream_id] = datetime(year=2021, month=1, day=1).timestamp()
                        
                        for date in dates:
                            if date > stream_ends[stream_id]:
                                stream_ends[stream_id] = date

                            to_save = {"date": date, "game_id":  game_id, "user_id": user_id, "stream_id": stream_id}
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
                                continue
                            
                            value = count[0][0]
                            c = count[0][1]

                            if value < 0:
                                continue
                            
                            if c > len(latency.keys()) / 2:
                                if "latency" not in to_save and has_mark:
                                    to_save["latency"] = str(value)
                                    to_save["values"] = values
                                                                                                        
                                    if self.controller.store_information(to_save):
                                        info_to_store[game_id].append(to_save)
                            
                            if c == len(latency.keys()) - 1 and len(count) > 1:
                                if has_mark: 
                                    partial_value = copy.deepcopy(to_save)
                                    if "latency" in partial_value:
                                        partial_value.pop("latency")

                                    partial_value["values"] = values

                                    if self.controller.store_alternative_values(partial_value):                                        
                                        alternative_values[game_id].append(partial_value)
                            
                            if c == int(len(latency.keys()) / 2):
                                to_save["real_latency"] = values
                                to_confirm.append(to_save)
    
            useful_per_game = {}
                    
            for game_id, values in info_to_store.items():
                useful_per_game[game_id] = len(values)

            # Once you are finally done, you need to save the metadata
            metadata_line = {"batch_name": batch_name, **metadata, "images_stats": {"total": parsers.get("easyocr").report_processed(), "useful": useful_per_game}}

            self.logger.info("Starting to compile all the results into a json")
            results_json_name = "{}-results.json".format(batch_name)
            with open(results_json_name, "w+") as f:
                f.write(json.dumps(metadata_line) + "\n")
                
                for game_id, values in info_to_store.items():
                    for value in values:
                        if "_id" in value:
                            value.pop("_id")
                        f.write(json.dumps(value) + "\n")

            alternatives_json_name = "{}-alternative-values.json".format(batch_name)
            with open(alternatives_json_name, "w+") as f:
                for game_id, values in alternative_values.items():
                    for value in values:
                        if "_id" in value:
                            value.pop("_id")
                        f.write(json.dumps(value) + "\n")

            jsons_to_upload = [results_json_name, alternatives_json_name]

            if to_confirm:
                to_confirm_json_name = "{}-to_confirm.json".format(batch_name)  
                with open(to_confirm_json_name, "w+") as f:
                    for tc in to_confirm:
                        f.write("{}\n".format(json.dumps(tc)))

                jsons_to_upload.append(to_confirm_json_name)
                self.controller.store_to_confirm(batch_name)

            self.logger.info("Inserting metadata in Mongo")
            
            self.controller.store_metadata(metadata_line)
            self.controller.store_jsons(jsons_to_upload)
            self.controller.clean_up(metadata_line)

            self.controller.store_stream_ends(stream_ends, batch_name)

    
if __name__ == "__main__":
    processor = RawLatencyProcessor()
    processor.run()
