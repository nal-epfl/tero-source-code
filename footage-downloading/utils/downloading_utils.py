import os
from utils.base_utils import generate_token, get_chunks_metadata
from config import bucket_name


def get_metadata(mongo_client, stream_id):
    twitch_data = mongo_client.videos.twitch_data.find_one({"stream_id": stream_id}, projection={"_id": False})
    
    return twitch_data



def get_chunk_metadata(mongo_client, stream_id, video_id):
    chunk_data = mongo_client.videos.chunks_metadata.find_one({"stream_id": stream_id}, projection={"_id": False})
    if not chunk_data:
        chunks_metadata = get_chunks_metadata(video_id)
        mongo_client.videos.chunks_metadata.insert_one({"stream_id": stream_id, "metadata": chunks_metadata})

        return {"stream_id": stream_id, "metadata": chunks_metadata}

    return chunk_data


def upload_raw_image(s3_client, cache, logger, frame_name, image_path):
    image_token = generate_token()
    image_name = "{}_{}_0.{}".format(frame_name.split("_")[0], image_token, image_path.split(".")[-1])
            
    s3_image_path = "raw/{}".format(image_name)
    response = s3_client.upload_file(image_path, bucket_name, s3_image_path)
    
    cache.sadd("raw_images", s3_image_path)
    
    if os.path.isfile(image_path):
        os.remove(image_path)

    return image_token