import os
import re
import boto3

from config import jumphost, jumphost_user, jumphost_password, tiny_img_path, tiny_results_path, tiny_to_process_path, rw_access_key, rw_secret_key, s3_url, bucket_name, redis_host, redis_password, mongo_host, mongo_user, mongo_password, remote_path

def get_results_name(file_info):
        return {"result": "{}_confirmation-{}.json".format(file_info["date"], file_info["technique"]), "log": "{}_log-{}.txt".format(file_info["date"], file_info["technique"])}


def parse_file_name(file_name):
        m = re.search(r"(?P<date>\d\d\d\d-\d\d-\d\d-\d\d-\d\d-\d\d)_log-(?P<technique>\w+)", file_name)
        if m:
            return {"date": m.group("date"), "technique": m.group("technique")}


if __name__ == "__main__":
    s3_client = boto3.client(
            's3',
            aws_access_key_id=rw_access_key,
            aws_secret_access_key=rw_secret_key,
            endpoint_url=s3_url
        )

    result_logs = [parse_file_name(x) for x in os.listdir(tiny_results_path) if "log" in x]
    
    results_dict = {}

    for to_process in result_logs:
        if to_process["date"] not in results_dict:
            results_dict[to_process["date"]] = []

        results_dict[to_process["date"]].append(to_process)

    results_to_upload = {}

    for date, corresponding_results in results_dict.items():    
        if len(corresponding_results) == 3:
            results_to_upload[date] = corresponding_results

    
    for name, results in results_to_upload.items():
        # Get original information
        # Upload jsons with raw results to S3 and delete logs
        for result in results:                
            print(result)
    
        if len(results) == 3:
            # I managed to upload the 3 files correctly
            print("to cache: {}".format(results))