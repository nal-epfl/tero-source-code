import os
import json
import boto3

from pymongo import MongoClient
from dateutil import tz
from datetime import datetime, timedelta
from config import bucket_name, rw_access_key, rw_secret_key, s3_url, mongo_host, mongo_port, mongo_user, mongo_password
from logger import get_logger


class BackupMongo:
    def __init__(self):
        self.logger = get_logger("backup_mongo")
    
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=rw_access_key,
            aws_secret_access_key=rw_secret_key,
            endpoint_url=s3_url
        )

        self.mongo_client = MongoClient("mongodb://{}:{}/".format(mongo_host, mongo_port),
                                        username=mongo_user,
                                        password=mongo_password)

        self.now = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')


    def backup_location_collection(self, col):
        backup_file = "mongo-location-{}-{}.json".format(col, self.now)

        self.logger.info("Fetching: location.{}".format(col))

        with open(backup_file, "w+") as f:
            for entry in self.mongo_client.location[col].find(projection={"_id": False}):
                f.write("{}\n".format(json.dumps(entry)))

        objects = self.s3_client.list_objects(Bucket=bucket_name, Prefix="backups/mongo-location-{}".format(col))
        if "Contents" in objects:
            for idx, ob in enumerate(objects["Contents"]):
                if ob["LastModified"] < datetime.now(tz=tz.tzutc()) - timedelta(days=8):
                    self.s3_client.delete_object(Bucket=bucket_name, Key=ob["Key"])
                    self.logger.info("Deleting: {}".format(ob["Key"]))

        self.logger.info("Uploading: {}".format(backup_file))
        self.s3_client.upload_file(backup_file, bucket_name, "backups/{}".format(backup_file))

        if os.path.isfile(backup_file):
            os.remove(backup_file)



    def backup_latency_collection(self, col):
        backup_file = "mongo-latency-{}-{}.json".format(col, self.now)

        self.logger.info("Fetching: latency.{}".format(col))

        with open(backup_file, "w+") as f:
            for entry in self.mongo_client.data[col].find(projection={"_id": False}):
                f.write("{}\n".format(json.dumps(entry)))

        objects = self.s3_client.list_objects(Bucket=bucket_name, Prefix="backups/mongo-latency-{}".format(col))
        if "Contents" in objects:
            for idx, ob in enumerate(objects["Contents"]):
                if ob["LastModified"] < datetime.now(tz=tz.tzutc()) - timedelta(days=2):
                    self.s3_client.delete_object(Bucket=bucket_name, Key=ob["Key"])
                    self.logger.info("Deleting: {}".format(ob["Key"]))

        self.logger.info("Uploading: {}".format(backup_file))
        self.s3_client.upload_file(backup_file, bucket_name, "backups/{}".format(backup_file))

        if os.path.isfile(backup_file):
            os.remove(backup_file)


    def run(self):
        for col in self.mongo_client.location.list_collection_names():
           self.backup_location_collection(col) 

        for col in self.mongo_client.data.list_collection_names():
           self.backup_latency_collection(col) 



if __name__ == "__main__":
    backup = BackupMongo()
    backup.run()

