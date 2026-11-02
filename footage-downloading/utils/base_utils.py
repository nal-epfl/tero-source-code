import requests
import subprocess
import re

from uuid import uuid4
from datetime import timedelta
from utils.twitch_video_utils import get_access_token, get_playlists, parse_playlists, get_correct_playlist


def get_chunks_metadata(video_id):    
    # Given a video id, get the access token 
    access_token = get_access_token(video_id)
    if not access_token:
        return []

    playlists_m3u8 = get_playlists(video_id, access_token)

    # Parse the m3u8 file to get the playlist URLs
    playlists = list(parse_playlists(playlists_m3u8))

    if not playlists:
        return []

    # Ideally, the quality will always be 1080p to keep our standard, but figure out if it 
    playlist_url = get_correct_playlist(playlists)

    if not playlist_url:
        return []

    response = requests.get(playlist_url)
    
    base_uri = re.sub("/[^/]+$", "/", playlist_url)

    if response.status_code == 200:
        # return {"playlist": m3u8.loads(response.text), "base_uri": base_uri}
        return {"playlist": response.text, "base_uri": base_uri}
    

def get_real_timestamp(broadcast_start, internal_ts):
    return (broadcast_start + timedelta(seconds=internal_ts)).timestamp()


# -hwaccel cuda
def extract_first_frame(video_path, output_path):
    # '-v', 'quiet',
    result = subprocess.run(['ffmpeg', '-hwaccel', 'cuda', '-y', '-v', 'quiet', '-i', video_path, '-vf', r"select=eq(n\,0)", '-vframes', '1', output_path], stdout=subprocess.DEVNULL)
    return result.stdout



# To extract frames directly from the URL (skip the disk bottleneck)
# def download_twitch_video(self, chunk_to_download, playlist_metadata, file_name):    
#         # Get the base URL that contains bget_chunks_metadataoth the index and all chunks.    
#         source = playlist_metadata["base_uri"] + chunk_to_download["segment_uri"]
        
#         start_time = timeit.default_timer()    
#         subprocess.run(['ffmpeg', '-y', '-v', 'quiet', '-i', source, '-vf', 'fps=1', "{}/{}-%02d.png".format(base_frame_destination_path, file_name)], stdout=subprocess.DEVNULL)
#         end_time = timeit.default_timer()    
        
#         self.logger.info("Time to download: {:.2f}".format(end_time - start_time))
#         self.cache.sadd("chunk-time-lower-load", end_time - start_time)



# '-hwaccel', 'cuda',
def extract_frame_every_second(video_path, output_path):
    # '-v', 'quiet',
    result = subprocess.run(['ffmpeg', '-y', '-v', 'quiet', '-i', video_path, '-vf', 'fps=1', "{}-%02d.png".format(output_path)], stdout=subprocess.DEVNULL)
    return result.stdout


def generate_token():
    rand_token = uuid4()
    return rand_token.hex


def parse_image_name(image_name):
    m = re.search(r"(?P<game>\d+)_(?P<user_id>\w+)_(?P<stream_id>\w+)_(?P<date>\d+)", image_name)
    if m:
        return {"game_id": m.group("game"), "date": m.group("date"), "stream_id": m.group("stream_id"), "user_id": m.group("user_id")}

    
def is_in_qoe_band(point, last_qoe_band):
    return last_qoe_band["min"] <= int(point["latency"]) and int(point["latency"]) <= last_qoe_band["max"] 
