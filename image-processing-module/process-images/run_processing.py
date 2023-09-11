import sys
import os
import json
import re
import redis
import boto3
import shutil

from zipfile import ZipFile
from datetime import datetime

from config import redis_host, redis_port, redis_password, batch_size, s3_url, rw_access_key, rw_secret_key, bucket_name, results_storage, img_storage, to_process_storage, processing_queues, max_simultaneous, long_term_storage
from logger import get_logger


class ProcessingRunner:
    def __init__(self):
        self.logger = get_logger("run_processing")
        self.cache = redis.Redis(host=redis_host, port=redis_port, password=redis_password)
        self.client = boto3.client(
            's3',
            aws_access_key_id=rw_access_key,
            aws_secret_access_key=rw_secret_key,
            endpoint_url=s3_url
        )

        self.delete_all = False
        self.add_new = True


    def clean_local(self, name):
        self.logger.info("Checking previous results: cleaning images associated with {}".format(name))
        images_file_abs_path = "{}/{}.txt".format(to_process_storage, name)

        with open(images_file_abs_path, "r") as f:
            for l in f:
                file_name = "{}/{}".format(img_storage, l.strip())

                if os.path.isfile(file_name):
                    os.remove(file_name)
        
        if os.path.isfile(images_file_abs_path):
            os.remove(images_file_abs_path)


    @staticmethod
    def parse_file_name(file_name):
        m = re.search(r"(?P<date>\d\d\d\d-\d\d-\d\d-\d\d-\d\d-\d\d)_log-(?P<technique>\w+)", file_name)
        if m:
            return {"date": m.group("date"), "technique": m.group("technique")}


    @staticmethod
    def get_results_name(file_info):
        return {"result": "{}_matches-{}.json".format(file_info["date"], file_info["technique"]), "log": "{}_log-{}.txt".format(file_info["date"], file_info["technique"])}


    def upload_and_clean(self, result, contents):
        s3_json_name = "raw_latency/{}".format(result["result"])
        log_absolute_path = "{}/{}".format(results_storage, result["log"])

        try:
            absolute_path = "{}/{}".format(results_storage, result["result"])
            
            self.client.upload_file(absolute_path, bucket_name, s3_json_name)
            contents["results"].append(s3_json_name)

            self.logger.info("Checking previous results: Uploaded {} to S3".format(result["result"]))

            # Delete the result files or move them to a long term storage directory (depending on your configurations)
            if self.delete_all:
                if os.path.isfile(absolute_path):
                    os.remove(absolute_path)
            else:
                if os.path.isfile(absolute_path):
                    shutil.move(absolute_path, "{}/{}".format(long_term_storage, result["result"]))

            # Delete the log file
            if os.path.isfile(log_absolute_path):
                os.remove(log_absolute_path)
            
        except Exception:
            # This time I'll pass
            pass


    def check_if_finished(self, name, contents):
        if len(contents["results"]) == len(processing_queues):
            # I managed to upload the 4 files correctly
    
            self.logger.info("Checking previous results: Deleting entry {} from cache and inserting information to post-process".format(name))

            self.cache.hdel("currently_processing", name)
            self.cache.sadd("to_postprocess", json.dumps(contents))
        else:
            # At least one missing json that will be uploaded later
            self.logger.info("Checking previous results: Missing jsons for {}, keeping information in cache".format(name))

            self.cache.hset("currently_processing", name, json.dumps(contents))


    def run(self):
        to_process_lists = [os.path.join(to_process_storage, x) for x in os.listdir(to_process_storage)]

        result_logs = [ProcessingRunner.parse_file_name(x) for x in os.listdir(results_storage) if "log" in x]
        
        self.logger.info("Checking previous results: found {} logs".format(len(result_logs)))

        results_dict = {}

        for to_process in to_process_lists:
            name = to_process.split("/")[-1].split(".")[-2]
            corresponding_results = [ProcessingRunner.get_results_name(r) for r in result_logs if name in r["date"]]
            
            if len(corresponding_results) == len(processing_queues):
                results_dict[name] = corresponding_results

                # I have all 4 results associated with this name, I can now delete the bw images
                self.logger.info("Checking previous results: found {} jsons associated with file {}".format(len(corresponding_results), name))
                self.clean_local(name)

        for name, results in results_dict.items():
            # Get original information
            if not self.cache.hexists("currently_processing", name):
                continue

            contents = json.loads(self.cache.hget("currently_processing", name).decode("utf-8"))

            self.logger.info("Checking previous results: Uploading jsons to S3")

            # Upload jsons with raw results to S3 and delete logs
            for result in results:
                if result in contents["results"]:
                    # Skip this file as it has been uploaded before
                    continue
                
                self.upload_and_clean(result, contents)
        
            self.check_if_finished(name, contents)

        possible_old = {}
        for k,v in self.cache.hgetall("currently_processing").items():
            possible_old[k.decode("utf-8")] = json.loads(v.decode("utf-8"))

        for name, contents in possible_old.items():
            if contents["results"]:
                for result in result_logs:
                    if result["date"] == name:
                        corresponding_result = ProcessingRunner.get_results_name(result) 
                        
                        if corresponding_result["result"] not in contents["results"]:
                            self.upload_and_clean(corresponding_result, contents)
                
                self.check_if_finished(name, contents)

        if not self.add_new:
            sys.exit(0)

        # Check if you have space to add an extra entry to process
        currently_processing = self.cache.hlen("currently_processing")
        if currently_processing >= max_simultaneous:
            self.logger.info("Stopping before adding a new entry to process. Number of elements currently being processed: {}".format(currently_processing))
            sys.exit(0)

        to_process = [x.decode("utf-8") for x in self.cache.spop("zip_to_process", count=batch_size)]

        self.logger.info("Found {} zips to process".format(len(to_process)))

        if not to_process:
            self.logger.info("Nothing to process: finishing")
            sys.exit(0)

        now = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        actually_downloaded = []

        with open("{}/{}.txt".format(to_process_storage, now), "w+") as f:
            for idx, zip_file in enumerate(to_process):
                tmp_zip_name = "tmp_to_process_{}.zip".format(idx)

                try:    
                    self.client.download_file(bucket_name, zip_file, tmp_zip_name)
                    self.logger.info("Downloaded {} from S3".format(zip_file))

                    with ZipFile(tmp_zip_name, 'r') as zip_ref:
                        zip_ref.extractall(img_storage)
                    
                    for fiz in zip_ref.namelist():
                        f.write("{}\n".format(fiz))

                    if os.path.isfile(tmp_zip_name):
                        os.remove(tmp_zip_name)
                    
                    actually_downloaded.append(zip_file)
                except Exception:
                    self.logger.info("Couldn't download {} from S3, returning zip entry to cache".format(zip_file))
                    self.cache.sadd("zip_to_process", zip_file)
        
        if actually_downloaded:
            self.logger.info("Adding new entry to the cache: {}".format(now))

            self.cache.hset("currently_processing", now, json.dumps({"contents": actually_downloaded, "results": []}))

            for q in processing_queues:
                self.cache.sadd(q, now)
        else:
            self.logger.info("No zip could be downloaded, deleting empty file: {}".format(now))
            
            if os.path.isfile("{}/{}.txt".format(to_process_storage, now)):
                os.remove("{}/{}.txt".format(to_process_storage, now))



if __name__ == "__main__":
    runner = ProcessingRunner()
    runner.run()

