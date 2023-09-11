from datetime import datetime


class StreamToProbe:
    def __init__(self, stream):
        self.stream_id = stream['id']
        self.user_id = stream['user_id']
        self.url = stream['thumbnail_url']
        self.next_time = datetime.now()
