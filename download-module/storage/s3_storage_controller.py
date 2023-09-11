import boto3

from storage.storage_controller import StorageController
from config import s3_url, rw_access_key, rw_secret_key, bucket_name, raw_images_path


class S3StorageController(StorageController):
    def __init__(self):
        super().__init__()     
        self.client = boto3.client(
            's3',
            aws_access_key_id=rw_access_key,
            aws_secret_access_key=rw_secret_key,
            endpoint_url=s3_url
        )
                
    def save_image(self, stream, game_id, thumbnail_date, image_data):        
        file_path = "{}/{}_{}_{}_{}.png".format(raw_images_path, game_id, thumbnail_date.strftime('%Y-%m-%d-%H-%M-%S'), stream.stream_id, stream.user_id)       
        self.client.put_object(Body=image_data, Bucket=bucket_name, Key=file_path)

        return file_path