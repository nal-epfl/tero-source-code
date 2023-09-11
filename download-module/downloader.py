import requests
import email.utils as eut
import sys
import redis
import random
import json
from storage.local_storage_controller import LocalStorageController
from storage.s3_storage_controller import S3StorageController

from stream_to_probe import StreamToProbe
from time import sleep
from datetime import datetime, timedelta
from logger import get_logger
from config import width, height, default_to_sleep, redis_host, redis_password, redis_port, downloader_fetch_count


def parse_http_date(date_str):
    return datetime.fromtimestamp(eut.mktime_tz(eut.parsedate_tz(date_str)))


class Downloader():
    def __init__(self, idx=0):
        super().__init__()
        self.idx = idx
        self.logger = get_logger("thumbnails_process{}".format(idx), 'downloader_{}'.format(idx))
        self.queue = redis.Redis(host=redis_host, port=redis_port, db=0, password=redis_password)
        self.streams = {}
        self.storage = LocalStorageController()

        self.update_streams()
        

    def update_streams(self):
        streams = self.queue.smembers("streams_{}".format(self.idx))
        self.logger.info("Re-read queue, current number of streams: {}".format(len(streams)))
        
        for s in streams:
            stream_data = self.queue.hget("current_probes", s)
            if stream_data:
                json_data = json.loads(stream_data)

                self.streams[json_data["id"]] = StreamToProbe(json_data)
            else:
                self.queue.srem("streams_{}".format(self.idx), s)
        

    def cleanup(self):
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)


    def run(self):
        while True:
            # If my queue has been empty for X cycles, I should stop iterating and just exit
            if self.queue.scard("streams_{}_stop".format(self.idx)) != 0:
                self.queue.spop("streams_{}_stop".format(self.idx), count=self.queue.scard("streams_{}_stop".format(self.idx)))
                self.logger.info("Got a stop signal, finishing.")
                break
    
            if self.queue.scard("streams_{}".format(self.idx)) != len(self.streams.keys()):
                self.streams = {}
                self.update_streams()

            should_slowdown = False
            finished = []

            streams_to_download = sorted(self.streams.values(), key=lambda x: x.next_time)
            for stream in streams_to_download:
                if stream.next_time < datetime.now():                
                    too_fast, has_finished = self.download_thumbnail(stream)
                    
                    if has_finished:
                        finished.append((stream.user_id, stream.stream_id))                   
                    else:
                        self.logger.info("Downloaded {}, next time: {}".format(stream.stream_id, stream.next_time.strftime('%Y-%m-%d-%H-%M-%S')))        
               
                    should_slowdown = should_slowdown & too_fast                

            if finished:
                for f in finished:
                    self.streams.pop(f[1])
                    self.queue.srem("streams_{}".format(self.idx), f[1])

                msg = {"origin": self.idx, "finished": finished}

                self.queue.sadd("from_workers", json.dumps(msg))
            
            new_streams = self.queue.spop("to_download", count=downloader_fetch_count)

            if new_streams:
                for new_stream in new_streams:
                    stream = json.loads(new_stream.decode("utf-8"))

                    self.streams[stream["id"]] = StreamToProbe(stream)
                    self.queue.sadd("streams_{}".format(self.idx), stream["id"]) 

                self.logger.info("Got new streams to follow, current total: {}".format(len(self.streams.keys())))

            sleep(default_to_sleep)


    def download_thumbnail(self, stream):
        url = stream.url.format_map({'width': width, 'height': height})
        
        try:
            status_response = requests.head(url, timeout=5)
            
            if status_response.status_code != 200:
                # Streamer has finished streaming
                self.logger.info("Streamer has finished {}. Status code: {}".format(stream.stream_id, status_response.status_code))
                return True, True

            thumbnail_date = parse_http_date(status_response.headers['Date'])
            
            too_fast = False
            if datetime.now() - stream.next_time > timedelta(minutes=10):
                self.logger.info("Too late downloading {}, should slowdown".format(stream.stream_id))
                too_fast = True

            stream.next_time = parse_http_date(status_response.headers['Expires'])
            
            response = requests.get(url, timeout=5)
            
            stream_data = self.queue.hget("current_probes", stream.stream_id)
            if stream_data:
                fresh_data = json.loads(stream_data)

                file_path = self.storage.save_image(stream, fresh_data["game_id"], thumbnail_date, response.content)
                self.queue.sadd("raw_images", file_path)
                self.queue.sadd("img_download_log", json.dumps({"expected": thumbnail_date.timestamp(), "downloaded": datetime.now().timestamp()}))

            return too_fast, False
        except Exception as e:
            self.logger.info("Fatal error downloading {}. Error: {}".format(stream.stream_id, e))
            
            return False, False


if __name__ == "__main__":
    if len(sys.argv) == 2: 
        downloader = Downloader(sys.argv[1])
        downloader.run()
