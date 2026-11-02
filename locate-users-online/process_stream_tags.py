import os
import json
import pycountry
import redis
import boto3

from pymongo import MongoClient, ReturnDocument
from datetime import datetime
from collections import Counter
from config import redis_host, redis_password, mongo_host, mongo_password, mongo_user, rw_access_key, rw_secret_key, s3_url, bucket_name, base_path
from logger import get_logger



class ProcessStreamTags:
    def __init__(self) -> None:
        self.mongo_client = MongoClient("mongodb://{}:27017/".format(mongo_host), username=mongo_user, password=mongo_password)  
        self.logger = get_logger("process_tags")
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=rw_access_key,
            aws_secret_access_key=rw_secret_key,
            endpoint_url=s3_url
        )


    def format_country(self, country_data):
        return {"country_code": country_data.alpha_2.lower(), "country": country_data.name}


    def process_file(self, file_name):
        self.logger.info("Processing file.")
        streams_with_tags = {}

        content_object = self.s3_client.get_object(Bucket=bucket_name, Key="streams/{}".format(file_name))
        file_content = content_object['Body'].read().decode('utf-8')
        lines = file_content.split("\n")

        for l in lines:
            if not l.strip():
                continue

            try:
                data = json.loads(l.strip())
            except Exception:
                continue
    
            for stream in data["data"]:
                tags = stream.get("tags", [])
                
                if not tags:
                    continue
                
                user_country_tags = []

                for tag in tags:
                    try:
                        country_data = pycountry.countries.lookup(tag)
                        user_country_tags.append(self.format_country(country_data))
                    except LookupError:
                        continue

                if user_country_tags:
                    stream.pop("thumbnail_url")
                    stream.pop("tag_ids")
                    stream.pop("started_at")
                    stream.pop("viewer_count")
                    stream.pop("type")
                    stream.pop("game_id")
                    stream.pop("game_name")
                    stream.pop("is_mature")

                    if stream["user_id"] not in streams_with_tags:
                        streams_with_tags[stream["user_id"]] = []

                    streams_with_tags[stream["user_id"]].append({"tags": stream["tags"], "countries": user_country_tags})

        return streams_with_tags

    def flatten_time(self, time):
        return time if isinstance(time, float) else time.timestamp()


    def compile_tags(self, file_date, streams_with_tags):
        self.logger.info("Compiling tags. Users: {}".format(len(streams_with_tags.keys())))

        for user_id, new_data in streams_with_tags.items():
            compiled_results = self.mongo_client.location_data.tags.find_one({"user_id": user_id}, projection={"_id": False})
                
            if not compiled_results:
                compiled_results = {"user_id": user_id, "countries": {}}
            else:                
                extended_tags = {}
                for country, country_data in compiled_results["countries"].items():
                    extended = []
                    for t in country_data["tags"]:
                        extended.extend([t[0] for _ in range(0, t[1])])

                    extended_tags[country] = {"location": country_data["location"], "start": country_data["start"], "end": country_data["end"], "tags": extended}

                compiled_results["countries"] = extended_tags

            for data in new_data:
                for country in data["countries"]:
                    if country["country_code"] not in compiled_results["countries"]:
                        compiled_results["countries"][country["country_code"]] = {"location": country, "start": file_date, "end": file_date, "tags": []}

                    compiled_results["countries"][country["country_code"]]["tags"].extend(data["tags"])

                    if file_date.timestamp() < self.flatten_time(compiled_results["countries"][country["country_code"]]["start"]):
                        compiled_results["countries"][country["country_code"]]["start"] = file_date
                    elif file_date.timestamp() > self.flatten_time(compiled_results["countries"][country["country_code"]]["end"]):
                        compiled_results["countries"][country["country_code"]]["end"] = file_date

            countries_data = {}
            for country, data in compiled_results["countries"].items():
                countries_data[country] = {"location": data["location"], "start": self.flatten_time(data["start"]), "end": self.flatten_time(data["end"]), "tags": Counter(data["tags"]).most_common()}

            self.mongo_client.location_data.tags.find_one_and_replace({"user_id": user_id}, {"user_id": user_id, "countries": countries_data}, upsert=True, return_document=ReturnDocument.AFTER)


    def run(self, file_name):
        self.logger.info("Starting: {}".format(file_name))
        file_date = datetime.strptime(file_name.split(".")[0], "%Y-%m-%d-%H-%M-%S")

        stream_tags = self.process_file(file_name)
        self.compile_tags(file_date, stream_tags)
        self.logger.info("Finished processing file.")
    
    
from tqdm import tqdm

if __name__ =="__main__":
    tag_processor = ProcessStreamTags()

    cache = redis.Redis(host=redis_host, port=6379, db=0, password=redis_password)
    files_to_process = [x.decode("utf8") for x in cache.spop("stream_files", count=cache.scard("stream_files"))]
    # files_to_process = [x.decode("utf8") for x in cache.smembers("stream_files")]
            
    for ftp in tqdm(files_to_process):
        tag_processor.run(ftp)

