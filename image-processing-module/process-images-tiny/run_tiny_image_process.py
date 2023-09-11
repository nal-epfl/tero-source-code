import os
import re
import json
import boto3
import shutil
import redis

from zipfile import ZipFile
from pymongo import MongoClient
from datetime import datetime
from config import rw_access_key, rw_secret_key, s3_url, bucket_name, redis_host, redis_port, redis_password, mongo_host, mongo_port, queues, tiny_img_storage, tiny_to_process_path, tiny_results_storage, mongo_user, mongo_password
from logger import get_logger


class ConfirmationRunner:
    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=rw_access_key,
            aws_secret_access_key=rw_secret_key,
            endpoint_url=s3_url
        )

        self.mongo_client = MongoClient("mongodb://{}:{}/".format(mongo_host, mongo_port),
                                        username=mongo_user,
                                        password=mongo_password)
        self.cache = redis.Redis(host=redis_host, port=redis_port, password=redis_password)
        self.logger = get_logger("run_confirmation")


    def prepare_env(self):
        for path in [tiny_img_storage, tiny_to_process_path, tiny_results_storage]:
            if not os.path.isdir(path):
                os.makedirs(path)
        

    def fetch_to_confirm_file(self, date):
        content_object = self.s3_client.get_object(Bucket=bucket_name, Key="results/{}-to_confirm.json".format(date))
        file_content = content_object['Body'].read().decode('utf-8')
        return file_content.split("\n")


    def run(self):
        currently_processing = [x for x in os.listdir(tiny_to_process_path)]
        if len(currently_processing) > 20:
            self.logger.info("Currently processing 20, stopping process")
            return
        
        dates_to_process = [x.decode("utf-8") for x in self.cache.spop("to_confirm", count=10)]
        self.logger.info("Got to process: {}".format(dates_to_process))

        for to_confirm_date in dates_to_process:
            self.logger.info("Processing: {}".format(to_confirm_date))

            metadata = self.mongo_client.results.metadata.find_one({"batch_name": to_confirm_date}, projection={"_id": False})
            if not metadata:
                self.logger.info("Metadata not found: {}".format(to_confirm_date))
                continue

            lines = self.fetch_to_confirm_file(to_confirm_date)           

            files_to_keep = set()
            for l in lines:
                if l.strip():
                    data = json.loads(l.strip())
                    files_to_keep.add("{}_{}_{}_{}".format(data["game_id"], datetime.fromtimestamp(data["date"]).strftime("%Y-%m-%d-%H-%M-%S"), data["stream_id"], data["user_id"]))

            if not files_to_keep:
                self.logger.info("No files to confirm, continuing")
                continue

            self.logger.info("Files to confirm: {}".format(len(files_to_keep)))

            batch_storage_path = "{}/{}".format(tiny_img_storage, to_confirm_date)
            if not os.path.isdir(batch_storage_path):
                os.makedirs(batch_storage_path)
            
            for idx, batch in enumerate(metadata["contents"]):
                tiny_name = batch.replace("bw", "tiny")
                tmp_zip_name = "tmp_zip_{}.zip".format(idx)    

                try:
                    self.s3_client.download_file(bucket_name, tiny_name, tmp_zip_name)
                except Exception:
                    continue

                with ZipFile(tmp_zip_name, 'r') as zip_ref:
                    zip_ref.extractall(batch_storage_path)

                if os.path.isfile(tmp_zip_name):
                    os.remove(tmp_zip_name)

                for x in os.listdir(batch_storage_path):
                    if "extra" in x:
                        os.remove(os.path.join(batch_storage_path, x))
                        continue

                    just_prefix = x.split("_area")[0]
                    if just_prefix not in files_to_keep:
                        os.remove(os.path.join(batch_storage_path, x))
            
            self.logger.info("Writing to process: {}".format(to_confirm_date))
            with open("{}/{}.txt".format(tiny_to_process_path, to_confirm_date), "w+") as f:
                for x in os.listdir(batch_storage_path):
                    f.write("{}\n".format(x))
            
            for q in queues:
                self.cache.sadd(q, to_confirm_date)
            
            self.logger.info("Set to process on redis: {}".format(to_confirm_date))


if __name__ == "__main__":
    runner = ConfirmationRunner()
    runner.run()