import boto3
import cv2
import numpy as np

from botocore.client import Config
from storage.storage_controller import StorageController
from config import batch_size, s3_url, rw_access_key, rw_secret_key, bucket_name, increase_batch_threshold


class S3StorageController(StorageController):
    def __init__(self, idx):
        super().__init__(idx)

        self.client = boto3.client(
            's3',
            aws_access_key_id=rw_access_key,
            aws_secret_access_key=rw_secret_key,
            endpoint_url=s3_url,
            config=Config(connect_timeout=5, retries={'max_attempts': 0})
        )

    
    def get_images(self):
        if self.redis_cache.scard("raw_images") >= increase_batch_threshold * batch_size:
            return [x.decode("utf-8") for x in self.redis_cache.spop("raw_images", count=2*batch_size)]
        
        if self.redis_cache.scard("raw_images") >= batch_size:
            return [x.decode("utf-8") for x in self.redis_cache.spop("raw_images", count=batch_size)]

        return []

    def read_image(self, image, metadata):
        try:
            s3_obj = self.client.get_object(Bucket=bucket_name, Key=image)
            body = s3_obj['Body'].read()

            return cv2.imdecode(np.asarray(bytearray(body)), cv2.IMREAD_COLOR)
        except Exception:
            return None
       

    def clean_up_images(self, images, logger):
        logger.info("Deleting images from S3.")

        deleted_images = []

        for image in images:
            try:
                self.client.delete_object(Bucket=bucket_name, Key=image)
                deleted_images.append(image)
            except Exception as e:
                deleted_images.append(image)
               
        logger.info("Deleted {}/{} images from S3.".format(len(deleted_images), len(images)))

        return deleted_images


    def on_failure_zip(self, images, logger):    
        logger.info("Returning all images to redis.")
        self.redis_cache.sadd("raw_images", *images)


    def on_failure_delete(self, not_deleted, logger):
        if not_deleted:
            logger.info("Returning {} images to redis.".format(len(not_deleted)))
            self.redis_cache.sadd("raw_images", *not_deleted)