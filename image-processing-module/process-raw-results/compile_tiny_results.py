import os
import re
import json
import boto3
import shutil

import copy
from parsers import *
from collections import Counter
from logger import get_logger
from tqdm import tqdm

from db.online_controller import OnlineController
from config import rw_access_key, rw_secret_key, s3_url, tiny_results_path, long_term_storage, tiny_to_process_path, tiny_img_storage, bucket_name


class TinyResultsCompiler:
    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=rw_access_key,
            aws_secret_access_key=rw_secret_key,
            endpoint_url=s3_url
        )
        
        self.logger = get_logger("compile_confirmation")
        self.controller = OnlineController(self.logger)


    def parse_file_name(self, file_name, tag="log"):
        m = re.search(r"(?P<date>\d\d\d\d-\d\d-\d\d-\d\d-\d\d-\d\d)_{}-(?P<technique>\w+)".format(tag), file_name)
        if m:
            return {"date": m.group("date"), "technique": m.group("technique")}
        
        m = re.search(r"(?P<date>\d\d\d\d-\d\d-\d\d-\d\d-\d\d)_{}-(?P<technique>\w+)".format(tag), file_name)
        if m:
            return {"date": m.group("date"), "technique": m.group("technique")}



    def parse_json_name(self, json_name):
        m = re.search(r"(?P<date>\d\d\d\d-\d\d-\d\d-\d\d-\d\d-\d\d)_matches-(?P<engine>\w+).json", json_name)
        if m:
            return {"date": m.group("date"), "engine": m.group("engine")}

        m = re.search(r"(?P<date>\d\d\d\d-\d\d-\d\d-\d\d-\d\d)_matches-(?P<engine>\w+).json", json_name)
        if m:
            return {"date": m.group("date"), "engine": m.group("engine")}


    def clean_images(self, date):
        if os.path.isfile("{}/{}.txt".format(tiny_to_process_path, date)):
            os.remove("{}/{}.txt".format(tiny_to_process_path, date))

        if os.path.isdir("{}/{}".format(tiny_img_storage, date)):
            shutil.rmtree("{}/{}".format(tiny_img_storage, date))


    def fetch_to_confirm_file(self, date):
        content_object = self.s3_client.get_object(Bucket=bucket_name, Key="results/{}-to_confirm.json".format(date))
        file_content = content_object['Body'].read().decode('utf-8')
        return file_content.split("\n")


    def run(self):
        grouped_results = {}

        for x in os.listdir(tiny_results_path):   
            if "log" in x:
                parsed_name = self.parse_file_name(x)

                if parsed_name["date"] not in grouped_results:
                    grouped_results[parsed_name["date"]] = set()
                
                grouped_results[parsed_name["date"]].add(parsed_name["technique"])
        
        to_process = []
        for date, techniques in grouped_results.items():
            if len(techniques) == 3:
                to_process.append(date)
                self.clean_images(date)

        for batch_name in tqdm(to_process):
            parsers = {
                "easyocr": EasyOCRParser("tiny"),
                "paddleocr": PaddleOCRParser("tiny"),
                "pytesseract": PytesseractParser("tiny")
            }

            all_data_keys = {}
                
            for technique in grouped_results[batch_name]:
                file_name = "{}_confirmation-{}.json".format(batch_name, technique)
                
                lines = []
                
                with open("{}/{}".format(tiny_results_path, file_name), "r") as f:            
                    lines = f.readlines()
                    
                parser = parsers.get(technique)        
                parser.parse_file(lines, all_data_keys)
                
                stored_json = "{}/{}".format(long_term_storage, file_name)

                try:
                    s3_json_name = "results/{}".format(file_name)
                    
                    self.logger.info("Uploading to S3: {}".format(s3_json_name))
                    self.s3_client.upload_file("{}/{}".format(tiny_results_path, file_name), bucket_name, s3_json_name)
                    
                    shutil.move("{}/{}".format(tiny_results_path, file_name), stored_json)   
                except Exception as e:
                    self.logger.info("Error: {}. Json will be stored in {}".format(e, stored_json))
                
                                
                log_file = "{}/{}_log-{}.txt".format(tiny_results_path, batch_name, technique)
                if os.path.isfile(log_file):
                    os.remove(log_file)
            
            for parser in parsers.values():
                parser.post_process()      

            previous_results = {}
            lines = self.fetch_to_confirm_file(batch_name)           
                
            for l in lines:
                if l.strip():
                    data = json.loads(l.strip())
                    if data["game_id"] not in previous_results:
                        previous_results[data["game_id"]] = {}

                    if data["user_id"] not in previous_results[data["game_id"]]:
                        previous_results[data["game_id"]][data["user_id"]] = {}
                    
                    if data["stream_id"] not in previous_results[data["game_id"]][data["user_id"]]:
                        previous_results[data["game_id"]][data["user_id"]][data["stream_id"]] = {}
                    
                    if data["date"] not in previous_results[data["game_id"]][data["user_id"]][data["stream_id"]]:
                        previous_results[data["game_id"]][data["user_id"]][data["stream_id"]][data["date"]] = None

                    previous_results[data["game_id"]][data["user_id"]][data["stream_id"]][data["date"]] = data
        
            info_to_store = {}
            alternative_values = {}
            
            for game_id, users in all_data_keys.items():
                info_to_store[game_id] = []
                alternative_values[game_id] = []

                for user_id, streams in users.items():
                    for stream_id, dates in streams.items():
                        for date in dates:                
                            to_save = {"date": date, "game_id":  game_id, "user_id": user_id, "stream_id": stream_id}
                            has_mark = False
                            latency = {}
                            
                            previous_latency = previous_results.get(game_id, {}).get(user_id, {}).get(stream_id, {}).get(date, {}).get("real_latency", {})
                            for t, value in previous_latency.items():
                                latency[t] = int(value.get("latency", "-1"))

                                if value and t != "templates":
                                    has_mark = has_mark or value.get("has_mark", True)

                            for key, parser in parsers.items():
                                value = parser.get_value(to_save)
                                latency["{}_confirm".format(key)] = int(value.get("latency", "-1"))

                                if value and parser.name != "templates":
                                    has_mark = has_mark or value.get("has_mark", True)
                                                                
                            count = Counter(list(latency.values())).most_common()

                            value = count[0][0]
                            c = count[0][1]
                            
                            if value < 0:
                                continue

                            if c >= 3:
                                if "latency" not in to_save and has_mark:
                                    to_save["latency"] = str(value)
                                                                    
                                    if self.controller.store_information(to_save):
                                        info_to_store[game_id].append(to_save)                                   

                                if len(count) > 1:
                                    if has_mark: 
                                        partial_value = copy.deepcopy(to_save)
                                        if "latency" in partial_value:
                                            partial_value.pop("latency")

                                        partial_value["values"] = latency

                                        if self.controller.store_alternative_values(partial_value):                                        
                                            alternative_values[game_id].append(partial_value)
            
            useful_per_game = {}
            for game_id, values in info_to_store.items():
                useful_per_game[game_id] = len(values)

            self.logger.info("Starting to compile all the results into a json")
            results_json_name = "{}-confirmation_results.json".format(batch_name)
            with open(results_json_name, "w+") as f:                
                for game_id, values in info_to_store.items():
                    for value in values:
                        if "_id" in value:
                            value.pop("_id")
                        f.write(json.dumps(value) + "\n")

            alternatives_json_name = "{}-confirmation_alternative-values.json".format(batch_name)
            with open(alternatives_json_name, "w+") as f:
                for game_id, values in alternative_values.items():
                    for value in values:
                        if "_id" in value:
                            value.pop("_id")
                        f.write(json.dumps(value) + "\n")

            jsons_to_upload = [results_json_name, alternatives_json_name]

            self.logger.info("Inserting metadata in Mongo")
            
            self.controller.update_metadata(batch_name, useful_per_game)
            self.controller.store_jsons(jsons_to_upload)
                    
            self.logger.info("Process finished")


if __name__ == "__main__":
    runner = TinyResultsCompiler()
    runner.run()