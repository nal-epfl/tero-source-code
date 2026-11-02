import os
import re
import json
import boto3
import shutil
import redis

import copy
from parsers import *
from pymongo import MongoClient
from collections import Counter
from logger import get_logger
from tqdm import tqdm

from db.online_controller import OnlineController
from config import rw_access_key, rw_secret_key, s3_url, tiny_results_path, tiny_to_process_path, tiny_img_storage, bucket_name, mongo_host, redis_host, redis_password


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
        self.cache = redis.Redis(host=redis_host, port=6379, password=redis_password)


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


    def fetch_to_confirm_file(self, date):
        content_object = self.s3_client.get_object(Bucket=bucket_name, Key="results/{}-to_confirm.json".format(date))
        file_content = content_object['Body'].read().decode('utf-8')
        return file_content.split("\n")


    def process_image(self, previous_latency, parsers, to_save):
        to_return = {}

        has_mark = False
        latency = {}
                        
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
            return {}

        if c >= 3:
            if "latency" not in to_save and has_mark:
                to_save["latency"] = str(value)
                                                
                if self.controller.store_information(to_save):
                    to_return["info_to_store"] = to_save                       

            if len(count) > 1:
                if has_mark: 
                    partial_value = copy.deepcopy(to_save)
                    if "latency" in partial_value:
                        partial_value.pop("latency")

                    partial_value["values"] = latency

                    if self.controller.store_alternative_values(partial_value):
                        to_return["alternative_values"] = partial_value                                        

        return to_return


    def process_token_images(self, previous_results, keys, parsers):
        info_to_store = {}
        alternative_values = {}
    
        for game_id, tokens in keys.items():
            info_to_store[game_id] = []
            alternative_values[game_id] = []

            for token, indexes in tokens.items():
                for idx in indexes:              
                    to_save = {"game_id":  game_id, "token": token, "idx": idx}
                    previous_latency = previous_results.get(game_id, {}).get(token, {}).get(idx, {}).get("real_latency", {})
                        
                    processed = self.process_image(previous_latency, parsers, to_save)

                    if "info_to_store" in processed:
                        info_to_store[game_id].append(processed["info_to_store"])
                    else:
                        self.controller.store_information(to_save, always_insert=True)
                        info_to_store[game_id].append(to_save)

                    if "alternative_values" in processed:
                        alternative_values[game_id].append(processed["alternative_values"])
                                
        return info_to_store, alternative_values


    def process_twitch_images(self, previous_results, keys, parsers):
        info_to_store = {}
        alternative_values = {}

        for game_id, users in keys.items():
            info_to_store[game_id] = []
            alternative_values[game_id] = []

            for user_id, streams in users.items():
                for stream_id, dates in streams.items():
                    for date in dates:                
                        to_save = {"date": date, "game_id":  game_id, "user_id": user_id, "stream_id": stream_id}
                        previous_latency = previous_results.get(game_id, {}).get(user_id, {}).get(stream_id, {}).get(date, {}).get("real_latency", {})
                        
                        processed = self.process_image(previous_latency, parsers, to_save)

                        if "info_to_store" in processed:
                            info_to_store[game_id].append(processed["info_to_store"])

                        if "alternative_values" in processed:
                            alternative_values[game_id].append(processed["alternative_values"])
        
        return info_to_store, alternative_values


    def run(self):
        to_process = [x.decode("utf-8") for x in self.cache.spop("to_postprocess_tiny", count=self.cache.scard("to_postprocess_tiny"))]
        
        for batch_name in to_process:
            parsers = {
                "easyocr": EasyOCRParser("tiny"),
                "paddleocr": PaddleOCRParser("tiny"),
                "pytesseract": PytesseractParser("tiny")
            }

            all_data_keys = {}
            all_token_keys = {}
                
            for technique in parsers.keys():
                file_name = "results/{}_confirmation-{}.json".format(batch_name, technique)
                                            
                content_object = self.s3_client.get_object(Bucket=bucket_name, Key=file_name)
                file_content = content_object['Body'].read().decode('utf-8')
                lines = file_content.split("\n")
                                      
                parser = parsers.get(technique)        
                parser.parse_file(lines, all_data_keys, all_token_keys)
                
            for parser in parsers.values():
                parser.post_process()      

            previous_results = {}
            previous_token_results = {}
            lines = self.fetch_to_confirm_file(batch_name)           
                
            for l in lines:
                if l.strip():
                    data = json.loads(l.strip())

                    if "token" in data:
                        if data["game_id"] not in previous_token_results:
                            previous_token_results[data["game_id"]] = {}

                        if data["token"] not in previous_token_results[data["game_id"]]:
                            previous_token_results[data["game_id"]][data["token"]] = {}
                        
                        previous_token_results[data["game_id"]][data["token"]][data["idx"]] = data
                        continue

                    if data["game_id"] not in previous_results:
                        previous_results[data["game_id"]] = {}

                    if data["user_id"] not in previous_results[data["game_id"]]:
                        previous_results[data["game_id"]][data["user_id"]] = {}
                    
                    if data["stream_id"] not in previous_results[data["game_id"]][data["user_id"]]:
                        previous_results[data["game_id"]][data["user_id"]][data["stream_id"]] = {}
                    
                    if data["date"] not in previous_results[data["game_id"]][data["user_id"]][data["stream_id"]]:
                        previous_results[data["game_id"]][data["user_id"]][data["stream_id"]][data["date"]] = None

                    previous_results[data["game_id"]][data["user_id"]][data["stream_id"]][data["date"]] = data
        
            twitch_info_to_store, twitch_alternatives = self.process_twitch_images(previous_results, all_data_keys, parsers)
            token_info_to_store, token_alternatives = self.process_token_images(previous_token_results, all_token_keys, parsers)
            
            useful_per_game = {}
            for game_id, values in twitch_info_to_store.items():
                useful_per_game[game_id] = len(values)

            token_useful_per_game = {}
                
            for game_id, values in token_info_to_store.items():
                token_useful_per_game[game_id] = len(values)

            self.logger.info("Starting to compile all the results into a json")
            results_json_name = "{}-confirmation_results.json".format(batch_name)
            with open(results_json_name, "w+") as f:
                for d in [twitch_info_to_store, token_info_to_store]:                
                    for game_id, values in d.items():
                        for value in values:
                            if "_id" in value:
                                value.pop("_id")
                            f.write(json.dumps(value) + "\n")

            alternatives_json_name = "{}-confirmation_alternative-values.json".format(batch_name)
            with open(alternatives_json_name, "w+") as f:
                for d in [twitch_alternatives, token_alternatives]:
                    for game_id, values in d.items():
                        for value in values:
                            if "_id" in value:
                                value.pop("_id")
                            f.write(json.dumps(value) + "\n")

            jsons_to_upload = [results_json_name, alternatives_json_name]

            self.logger.info("Inserting metadata in Mongo")
            
            self.controller.update_metadata(batch_name, useful_per_game, token_useful_per_game)
            self.controller.store_jsons(jsons_to_upload)
                    
            self.logger.info("Process finished")


if __name__ == "__main__":
    runner = TinyResultsCompiler()
    runner.run()