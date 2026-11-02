import redis
import json
import requests
import os
import re
import m3u8
import boto3
import pytz
import sys
import timeit
import requests

from tqdm import tqdm
from datetime import datetime, timedelta
from dateutil import parser
from pymongo import MongoClient
from config import mongo_host, mongo_user, mongo_password, redis_host, redis_password, rw_access_key, rw_secret_key, s3_url, base_frame_destination_path
from utils.base_utils import get_real_timestamp, extract_frame_every_second, parse_image_name
from utils.downloading_utils import get_chunk_metadata, upload_raw_image, get_metadata


from logger import get_logger

localtz = pytz.timezone('Europe/Zurich')


class DownloadSamples:
    def __init__(self, idx):
        self.cache = redis.Redis(host=redis_host, port=6379, password=redis_password)
        self.mongo_client = MongoClient("mongodb://{}:27017/".format(mongo_host),
                            username=mongo_user,
                            password=mongo_password)
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=rw_access_key,
            aws_secret_access_key=rw_secret_key,
            endpoint_url=s3_url
        )
        self.logger = get_logger("download-chunks-{}".format(idx))
        self.latency_db = self.mongo_client.footage_data


    def download_twitch_video(self, chunk_to_download, playlist_metadata, file_name):    
        # Get the base URL that contains both the index and all chunks.    
        source = playlist_metadata["base_uri"] + chunk_to_download["segment_uri"]
        
        files_downloaded = []

        start_time = timeit.default_timer()
        with requests.get(source) as r:            
            with open("{}/{}".format(base_frame_destination_path, file_name), mode="wb") as file:
                file.write(r.content)
                files_downloaded.append("{}/{}".format(base_frame_destination_path, file_name))
                self.logger.info("Time to download {}: {:.2f} ({} bytes)".format(file_name, timeit.default_timer() - start_time, len(r.content)))
        
        return files_downloaded[0] if files_downloaded else None    


    def get_chunks_to_download(self, chunks_metadata, point_ts):
        playlist = m3u8.loads(chunks_metadata["metadata"]["playlist"])

        segments = []
        vod_start = 0

        for segment in playlist.segments:
            vod_end = vod_start + segment.duration
            segments.append({"segment_uri": segment.uri, "ts_start": vod_start, "ts_end": vod_end})
            vod_start = vod_end
        
        for idx, segment in enumerate(segments):
            contains_condition = segment["ts_start"] <= point_ts <= segment["ts_end"]
            if contains_condition:
                # Check if the chunk is even available in the first place
                chunk = requests.head("{}{}".format(chunks_metadata["metadata"]["base_uri"], segment["segment_uri"]))
                if chunk.status_code != 200:
                    # Store the stream as not available
                    self.logger.info("Stream unavailable: {}".format(chunks_metadata["stream_id"]))
                    self.cache.sadd("unavailable_streams", chunks_metadata["stream_id"])
                    return None, True

                return segment, False

        return None, False


    def convert_frame_name(self, frame_name):
        base_frame = frame_name.split("/")[-1]
        m = re.search(r"(?P<game>\d+)_(?P<user_id>\w+)_(?P<stream_id>\w+)_(?P<ts>\d+)-(?P<index>\d+)", base_frame)
        if m:            
            return "{}_{}_{}_{}.png".format(m.group("game"), m.group("user_id"), m.group("stream_id"), int(m.group("ts")) + int(m.group("index")) - 1)


    def run(self):
        batch_size = 10
        by_stream = {}

        for x in self.cache.smembers("chunks-to-download"):
            data = json.loads(x.decode("utf8"))

            if data["stream_id"] not in by_stream:
                by_stream[data["stream_id"]] = {}
            
            if data["ts_sample"] not in by_stream[data["stream_id"]]:
                by_stream[data["stream_id"]][data["ts_sample"]] = data

        selected_streams = {}
        chunks_deleted = 0

        for stream_id, stream_to_download in by_stream.items():
            if len(selected_streams) >= batch_size:
                break
            
            selected_streams[stream_id] = stream_to_download
            for s in stream_to_download.values():
                self.cache.srem("chunks-to-download", json.dumps(s))
                chunks_deleted += 1
        
        if chunks_deleted:
            self.logger.info("Chunks deleted: {}".format(chunks_deleted))

        for stream_id, stream_to_download in selected_streams.items():       
            # Check if the stream has been marked as "not available"
            if self.cache.sismember("unavailable_streams", stream_id):
                continue
            
            all_chunks = []    

            should_skip_stream = False
            for to_download in stream_to_download.values():
                if should_skip_stream:
                    break
                
                if self.mongo_client.footage.expected_batches.find_one({"game_id": to_download["game_id"], "user_id": to_download["user_id"], "stream_id": to_download["stream_id"], "inspected.ts": to_download["ts_sample"]}) or \
                self.mongo_client.footage.finished_batches.find_one({"game_id": to_download["game_id"], "user_id": to_download["user_id"], "stream_id": to_download["stream_id"], "inspected.ts": to_download["ts_sample"]}):
                    continue
               
                metadata = get_metadata(self.mongo_client, to_download["stream_id"])

                # Two alternatives: 
                # 1. If the stream has an entry but it is empty, it's over :(
                if metadata and not metadata["data"]:
                    continue
                
                broadcast_start = parser.parse(metadata["data"].get("node", {}).get("publishedAt", ""))
                video_id = metadata["data"].get("node", {}).get("id", "")
                if not video_id:
                    continue

                chunks_metadata = get_chunk_metadata(self.mongo_client, to_download["stream_id"], video_id)
                if not chunks_metadata["metadata"]:
                    continue
                
                # It is always one 
                chunk_to_download, should_skip_stream = self.get_chunks_to_download(chunks_metadata, to_download["ts_sample"] - broadcast_start.timestamp())
                if not chunk_to_download:
                    continue
                
                all_chunks.append({"td": to_download, "chunk": chunk_to_download})

            unique_chunks = {}
            for chunk in all_chunks:
                if chunk["chunk"]["segment_uri"] not in unique_chunks:
                    unique_chunks[chunk["chunk"]["segment_uri"]] = chunk

            self.logger.info("Unique chunks to download: {}".format(len(unique_chunks)))

            for chunk_data in unique_chunks.values():
                chunk_to_download = chunk_data["chunk"]
                to_download = chunk_data["td"]

                expected_dates = []

                date_to_download = int(get_real_timestamp(broadcast_start, int(chunk_to_download["ts_start"])))
                dt = datetime.fromtimestamp(date_to_download)

                expected_file_name = "{}_{}_{}_{}.png".format(to_download["game_id"], to_download["user_id"], to_download["stream_id"], date_to_download)            
                
                if self.mongo_client.footage.frames_downloaded.count_documents({"frame_name": expected_file_name}, limit=1) > 0:
                    continue

                if self.latency_db["latency_{}_{:02d}".format(dt.year, dt.month)].count_documents({"game_id": to_download["game_id"], "user_id": to_download["user_id"], "stream_id": to_download["stream_id"], "date": str(date_to_download)}, limit=1) > 0:
                    continue

                file_name = self.download_twitch_video(chunk_to_download, chunks_metadata["metadata"], "{}.ts".format(expected_file_name.split(".")[0]))
                if not file_name:
                    continue
                
                self.cache.sadd("chunk-downloaded", json.dumps({"ts": datetime.now().timestamp(), "file": expected_file_name}))

                extract_frame_every_second(file_name, file_name.split(".")[0])
                if os.path.isfile(file_name):
                    os.remove(file_name)               
                
                for idx in range(1, 11):
                    indexed_frame_name = "{}-{:02d}.png".format(file_name.split(".")[0], idx)

                    if not os.path.isfile(indexed_frame_name):
                        continue
                    
                    new_cache = redis.Redis(host=redis_host, port=6379, password=redis_password)

                    token = upload_raw_image(self.s3_client, new_cache, self.logger, os.path.basename(indexed_frame_name), indexed_frame_name)
                    converted_frame_name = self.convert_frame_name(os.path.basename(indexed_frame_name))

                    self.mongo_client.footage.frames_downloaded.insert_one({"frame_name": converted_frame_name, "token": token})
                    image_info = parse_image_name(converted_frame_name)

                    expected_dates.append(image_info["date"])
        
                if expected_dates:
                    self.mongo_client.footage.expected_batches.insert_one({"game_id": to_download["game_id"], "user_id": to_download["user_id"], "stream_id": to_download["stream_id"], 
                                                                                "inspected": {"min": to_download["min"], "max": to_download["max"], "ts": to_download["ts_sample"], "start": to_download["start"], "end": to_download["end"]},
                                                                                "expected_dates": expected_dates})
                        
        

if __name__ == "__main__":
    idx = 0
    if len(sys.argv) > 1:
        idx = sys.argv[1]

    sample_downloader = DownloadSamples(idx)
    sample_downloader.run()
