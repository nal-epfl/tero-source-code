import os
import re
import json
import boto3
import shutil
import redis
import paramiko

from scp import SCPClient

from zipfile import ZipFile
from pymongo import MongoClient
from datetime import datetime
from config import rw_access_key, rw_secret_key, s3_url, bucket_name, redis_host, redis_password, mongo_host, mongo_user, mongo_password, remote_path
from config import jumphost, jumphost_user, jumphost_password, tiny_img_path, tiny_results_path, tiny_to_process_path
from logger import get_logger

max_batches = 40


class ConfirmationRunner:
    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=rw_access_key,
            aws_secret_access_key=rw_secret_key,
            endpoint_url=s3_url
        )

        self.mongo_client = MongoClient("mongodb://{}:27017/".format(mongo_host),
                                        username=mongo_user,
                                        password=mongo_password)
        self.cache = redis.Redis(host=redis_host, port=6379, password=redis_password)
        self.logger = get_logger("run-tiny-images")

        self.jumphost = paramiko.SSHClient()
        self.connect_to_jumphost()

        self.scp_jumphost = SCPClient(self.jumphost.get_transport())


    def connect_to_jumphost(self):
        self.jumphost.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.jumphost.connect(jumphost, username=jumphost_user, password=jumphost_password, port=22, allow_agent=False,look_for_keys=False)


    def check_in_remote(self):
        in_remote = []
        stdin, stdout, stderr = self.jumphost.exec_command('ls {}/results'.format(remote_path))        

        for line in stdout.readlines():
            in_remote.append(line.strip())

        return in_remote


    def fetch_from_remote(self, to_fetch):
        self.scp_jumphost.get(remote_path="{}/results/{}".format(remote_path, to_fetch), local_path="{}/{}".format(tiny_results_path, to_fetch))
        

    def fetch_to_confirm_file(self, date):
        try:
            content_object = self.s3_client.get_object(Bucket=bucket_name, Key="results/{}-to_confirm.json".format(date))
            file_content = content_object['Body'].read().decode('utf-8')
            return file_content.split("\n")
        except Exception:
            self.logger.info("Failed fetching to-confirm file: {}".format(date))

    
    def move_zip_and_extract(self, zip_name, to_confirm_date):
        self.jumphost.exec_command("mkdir {}/images-in-process/{}".format(remote_path, to_confirm_date))
        self.scp_jumphost.put(zip_name, remote_path="{}/images-in-process/{}/{}".format(remote_path, to_confirm_date, zip_name))
        
        stdin, stdout, stderr = self.jumphost.exec_command("unzip {}/images-in-process/{}/{} -d {}/images-in-process/{}".format(remote_path, to_confirm_date, zip_name, remote_path, to_confirm_date))

        for line in stdout.readlines():
            pass

        # Remove the zip file once it is done
        self.jumphost.exec_command("rm {}/images-in-process/{}/{}".format(remote_path,  to_confirm_date, zip_name))        
        stdin, stdout, stderr = self.jumphost.exec_command("python3 {}/clean_not_needed.py {}".format(remote_path, to_confirm_date))

        for line in stdout.readlines():
            pass

    def clean_images(self, date):
        if os.path.isfile("{}/{}.txt".format(tiny_to_process_path, date)):
            os.remove("{}/{}.txt".format(tiny_to_process_path, date))

        if os.path.isdir("{}/{}".format(tiny_img_storage, date)):
            shutil.rmtree("{}/{}".format(tiny_img_storage, date))

    
    def move_file(self, file, destination):
        self.scp_jumphost.put(file, remote_path="{}/{}".format(destination, file.split("/")[-1]))

    
    def clean(self, name):
        self.logger.info("Checking previous results: cleaning images associated with {}".format(name))
        images_file_abs_path = "{}/{}.txt".format(tiny_to_process_path, name)
        
        if os.path.isdir("{}/{}".format(tiny_img_path, name)):
            for file in os.listdir("{}/{}".format(tiny_img_path, name)):
                file_name = "{}/{}/{}".format(tiny_img_path, name, file)

                if os.path.isfile(file_name):
                    os.remove(file_name)

            os.removedirs("{}/{}".format(tiny_img_path, name))

        if os.path.isfile(images_file_abs_path):
            os.remove(images_file_abs_path)
        
        self.jumphost.exec_command("rm -r {}/images-in-process/{}".format(remote_path, name))
        self.jumphost.exec_command("rm {}/to-process/{}.txt".format(remote_path, name))

    
    def delete_from_remote(self, to_remove):
        for tr in to_remove:
            self.jumphost.exec_command("rm {}/results/{}".format(remote_path, tr))      
            self.jumphost.exec_command("rm {}/results/{}".format(remote_path, tr.replace("matches", "log").replace(".json", ".txt")))   


    
    def upload_and_clean(self, result):
        json_name = "{}_confirmation-{}.json".format(result["date"], result["technique"])

        s3_json_name = "results/{}".format(json_name)
        log_absolute_path = "{}/{}_log-{}.txt".format(tiny_results_path, result["date"], result["technique"])

        try:
            absolute_path = "{}/{}".format(tiny_results_path, json_name)
            
            self.s3_client.upload_file(absolute_path, bucket_name, s3_json_name)
            self.logger.info("Checking previous results: Uploaded {} to S3".format(json_name))

            # Delete the result files or move them to a long term storage directory (depending on your configurations)
            if os.path.isfile(absolute_path):
                os.remove(absolute_path)

            # Delete the log file
            if os.path.isfile(log_absolute_path):
                os.remove(log_absolute_path)
        except Exception as e:
            print(e)
            pass
   
    @staticmethod
    def parse_file_name(file_name):
        m = re.search(r"(?P<date>\d\d\d\d-\d\d-\d\d-\d\d-\d\d-\d\d)_log-(?P<technique>\w+)", file_name)
        if m:
            return {"date": m.group("date"), "technique": m.group("technique")}
    

    @staticmethod
    def get_results_name(file_info):
        return {"result": "{}_confirmation-{}.json".format(file_info["date"], file_info["technique"]), "log": "{}_log-{}.txt".format(file_info["date"], file_info["technique"])}


    def run(self):
        in_remote = self.check_in_remote()
        to_remove_remote = []
        
        for file in in_remote:
            if "log" in file:
                # This means that a batch is done, set it for download
                file_name = file.replace("log", "confirmation").replace(".txt", ".json")
            
                self.fetch_from_remote(file_name)
                with open("{}/{}".format(tiny_results_path, file), "w+") as f:
                    f.write("Done")
                
                to_remove_remote.append(file)
                to_remove_remote.append(file_name)

                file_data = self.parse_file_name(file)
                self.cache.srem("{}_tiny".format(file_data["technique"]), file_data["date"])

        result_logs = [ConfirmationRunner.parse_file_name(x) for x in os.listdir(tiny_results_path) if "log" in x]
        results_dict = {}

        for to_process in result_logs:
            if to_process["date"] not in results_dict:
                results_dict[to_process["date"]] = []

            results_dict[to_process["date"]].append(to_process)

        results_to_upload = {}

        for date, corresponding_results in results_dict.items():    
            if len(corresponding_results) == 3:
                results_to_upload[date] = corresponding_results
                self.logger.info("Checking previous results: found {} jsons associated with file {}".format(len(corresponding_results), date))
                
                # I have all 3 results associated with this name, I can now delete the bw images
                self.clean(date)
                self.delete_from_remote(to_remove_remote)      

        # Upload jsons to S3        
        for name, results in results_to_upload.items():
            # Get original information
            self.logger.info("Checking previous results: Uploading jsons to S3")

            # Upload jsons with raw results to S3 and delete logs
            for result in results:                
                self.upload_and_clean(result)
        
            if len(results) == 3:
                # I managed to upload the 3 files correctly
                self.cache.sadd("to_postprocess_tiny", name)
            
        dates_to_process = sorted([x.decode("utf-8") for x in self.cache.smembers("to_confirm")], reverse=True)[:10]
        for d in dates_to_process:
            self.cache.srem("to_confirm", d)

        if dates_to_process:
            self.logger.info("Got to process: {}".format(dates_to_process))

        for to_confirm_date in dates_to_process:
            metadata = self.mongo_client.metadata.batches.find_one({"batch_name": to_confirm_date}, projection={"_id": False})
            if not metadata:
                self.logger.info("Metadata not found: {}".format(to_confirm_date))
                continue

            lines = self.fetch_to_confirm_file(to_confirm_date)
            if not lines:
                continue           

            files_to_keep = set()
            for l in lines:
                if l.strip():
                    data = json.loads(l.strip())

                    # Add token-based images
                    if "user_id" in data:
                        files_to_keep.add("{}_{}_{}_{}".format(data["game_id"], datetime.fromtimestamp(data["date"]).strftime("%Y-%m-%d-%H-%M-%S"), data["stream_id"], data["user_id"]))

                    if "token" in data:
                        files_to_keep.add("{}_{}_{}".format(data["game_id"], data["token"], data["idx"]))

            if not files_to_keep:
                continue

            batch_storage_path = "{}/{}".format(tiny_img_path, to_confirm_date)
            if not os.path.isdir(batch_storage_path):
                os.makedirs(batch_storage_path)
                        
            for idx, batch in enumerate(metadata["contents"]):
                tiny_name = batch.replace("bw", "tiny")
                tmp_zip_name = "tmp_zip_{}.zip".format(idx)    

                try:
                    self.s3_client.download_file(bucket_name, tiny_name, tmp_zip_name)
                except Exception:
                    continue

                try:
                    with ZipFile(tmp_zip_name, 'r') as zip_ref:
                        zip_ref.extractall(batch_storage_path)

                    for x in os.listdir(batch_storage_path):
                        if "extra" in x:
                            os.remove(os.path.join(batch_storage_path, x))
                            continue

                        just_prefix = x.split("_area")[0]
                        if just_prefix not in files_to_keep:
                            os.remove(os.path.join(batch_storage_path, x))
                
                    if os.path.isfile(tmp_zip_name):
                        os.remove(tmp_zip_name)                             
                except Exception as e:
                    print(e)
                    continue
            
            images_in_path =  [x for x in os.listdir(batch_storage_path)]

            if images_in_path:
                self.jumphost.exec_command("mkdir {}/images-in-process/{}".format(remote_path, to_confirm_date))    

                with open("{}/{}.txt".format(tiny_to_process_path, to_confirm_date), "w+") as f:
                    for x in images_in_path:
                        f.write("{}\n".format(x))
                        self.move_file("{}/{}".format(batch_storage_path, x), "{}/images-in-process/{}".format(remote_path, to_confirm_date))

            self.move_file("{}/{}.txt".format(tiny_to_process_path, to_confirm_date), "{}/to-process".format(remote_path))
            
            # for zip_name in zips_to_move:
                # self.move_zip_and_extract(zip_name, to_confirm_date)

                # if os.path.isfile(zip_name):
                #     os.remove(zip_name)

            queues = ["tesseract_tiny", "paddleocr_tiny", "easyocr_tiny"] #"paddleocr_tiny", "easyocr_tiny"
            for q in queues:
                self.cache.sadd(q, to_confirm_date)
            
            self.logger.info("Set to process on redis: {}".format(to_confirm_date))


if __name__ == "__main__":
    runner = ConfirmationRunner()
    runner.run()