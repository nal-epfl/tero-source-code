import os
import re
import json 
import shutil

from pymongo import MongoClient
from config import mongo_host, offline_storage_path, mongo_user, mongo_port, mongo_password


class OfflineController:
    def __init__(self, logger):
        self.logger = logger
        self.mongo_client = MongoClient("mongodb://{}:{}/".format(mongo_host, mongo_port),
                                        username=mongo_user,
                                        password=mongo_password)

        if not os.path.isdir(offline_storage_path):
            os.makedirs(offline_storage_path)


    def parse_json_name(self, json_name):
        m = re.search(r"(?P<date>\d\d\d\d-\d\d-\d\d-\d\d-\d\d-\d\d)-results.json", json_name)
        if m:
            return m.group("date")
        
        m = re.search(r"(?P<date>\d\d\d\d-\d\d-\d\d-\d\d-\d\d)-results.json", json_name)
        if m:
            return m.group("date")

    
    def get_batch_name(self, json_name):
        m = re.search(r"(?P<date>\d\d\d\d-\d\d-\d\d-\d\d-\d\d-\d\d)", json_name)
        if m:
            return m.group("date")
        
        m = re.search(r"(?P<date>\d\d\d\d-\d\d-\d\d-\d\d-\d\d)", json_name)
        if m:
            return m.group("date")


    def delete_zip(self, zip):
        pass

    
    def upload_json(self, prefix, json_name):
        shutil.move(json_name, "{}/{}".format(offline_storage_path, json_name))
            

    def get_to_process(self):
        return [x for x in self.mongo_client.results.metadata.aggregate([{"$sample": {"size": 1000}}, {"$project": {"_id": False}}])]
 
    
    def store_information(self, to_save):
        return True 

    
    def store_alternative_values(self, to_save):
        return True 
                
    
    def store_metadata(self, metadata_line):
        pass

    
    def store_jsons(self, results_jsons):
        # Upload the results to S3 for long-term storage
        for results_json_name in results_jsons:
            self.upload_json("results", results_json_name)


    def clean_up(self, metadata):
        for zip in metadata.get("contents", []):
            self.delete_zip(zip)


    def store_stream_ends(self, stream_ends, batch_name):
        with open("{}/{}-stream_ends.json".format(offline_storage_path, batch_name), "w+") as f:
            for stream, end in stream_ends.items():
                f.write(json.dumps({"stream_id": stream, "end": end}) + "\n")


    def store_to_confirm(self, batch_name):
        pass