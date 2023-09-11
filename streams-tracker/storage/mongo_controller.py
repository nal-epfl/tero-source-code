from pymongo import MongoClient
from config import mongo_host, mongo_port, mongo_user, mongo_password


class MongoController:
    def __init__(self):
        super().__init__()
        self.mongo_client = MongoClient('mongodb://{}:{}/'.format(mongo_host, mongo_port), username=mongo_user, password=mongo_password)
        
    # Stream + user + stream_start
    def save_stream_metadata(self, streams_metadata):
        # If the data is already there, do not store
        for stream_id, metadata in streams_metadata.items():
            if self.mongo_client.streams.metadata.count_documents({"stream_id": stream_id}, limit=1) == 0: 
                self.mongo_client.streams.metadata.insert_one(metadata)            


    def save_stream_data(self, data_to_store): 
        if data_to_store:
            self.mongo_client.streams.data.insert_many(data_to_store)


    def save_game_name_mapping(self, game_names):
        for game_id, game_name in game_names.items():
            if self.mongo_client.streams.game_names.count_documents({"game_id": game_id}, limit=1) == 0: 
                self.mongo_client.streams.game_names.insert_one({"game_id": game_id, "game_name": game_name})       