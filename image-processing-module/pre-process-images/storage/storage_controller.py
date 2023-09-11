import cv2
import os
import shutil
import redis

from datetime import datetime
from config import base_path, bucket_name, index_storage_path, redis_host, redis_port, redis_password, long_term_storage


class StorageController:
    def __init__(self, idx):
        self.now = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        self.redis_cache = redis.Redis(host=redis_host, port=redis_port, password=redis_password)
        self.local_buffer = {
            "bw": "{}/bw-{}".format(base_path, idx),
            "tiny": "{}/imgs-{}".format(base_path, idx)
        }
        
        self.create_local_tmp()


    def create_local_tmp(self):
        for _, dir_name in self.local_buffer.items():
            if not os.path.isdir(dir_name):
                os.makedirs(dir_name)
    
    def get_images(self):
        pass


    def read_image(self, image, metadata):
        pass

    
    def store_image(self, image, image_name, tag):
        path_to_store = self.local_buffer.get(tag, None)

        if path_to_store:
            path = "{}/{}".format(path_to_store, image_name)
            cv2.imwrite(path, image)

    
    def generate_zip(self, logger, delete=True):
        to_s3 = {}

        now = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        for tag, dir_name in self.local_buffer.items():
            if len(os.listdir(dir_name)) == 0:
                continue
            
            zip_name = "{}_{}".format(now, tag)
            
            shutil.make_archive(zip_name, 'zip', dir_name)
            to_s3[tag] = "{}.zip".format(zip_name)
        
        for tag, zip_file in to_s3.items():
            path_to_zip = "{}/{}".format(tag, zip_file)

            try:
                self.client.upload_file(zip_file, bucket_name, path_to_zip)
            except Exception:
                logger.info("Failure uploading {} to S3. Aborting operation".format(zip_file))

                return False

            if tag == "bw":
                self.redis_cache.sadd("zip_to_process", path_to_zip)

            logger.info("Uploaded {} to S3".format(path_to_zip))

        if delete:
            for _, zip_file in to_s3.items():
                if os.path.isfile(zip_file):
                    os.remove(zip_file)
        else:
            for _, zip_file in to_s3.items():
                if os.path.isfile(zip_file):
                    shutil.move(zip_file, "{}/{}".format(long_term_storage, zip_file))

        return now

    def clean_up_local(self, logger):
        logger.info("Cleaning up local tmp storage.")
        for _, dir_name in self.local_buffer.items():    
            shutil.rmtree(dir_name)

    
    def store_index(self):
        with open("{}/{}_index.txt".format(index_storage_path, datetime.now().strftime("%Y-%m-%d-%H-%M")), "w+") as f:
            for img in os.listdir(self.local_buffer.get("tiny", "")):
                f.write("{}\n".format(img))


    def clean_up_images(self, images_to_delete, logger):
        pass

    
    def on_failure_zip(self, images, logger):
        pass


    def on_failure_delete(self, not_deleted, logger):
        pass


    def finish(self, images, images_to_delete, logger):
        success = True
        success = self.generate_zip(logger, delete=False)
        
        if success:
            self.store_index()

        self.clean_up_local(logger)
        
        if success:       
            deleted_images = self.clean_up_images(images_to_delete, logger)
            not_deleted = list(set(images_to_delete) - set(deleted_images))          
            self.on_failure_delete(not_deleted, logger)
        else:
            self.on_failure_zip(images, logger)

        return success
