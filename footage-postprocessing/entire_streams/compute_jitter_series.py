import pandas as pd

from tqdm import tqdm
from datetime import datetime
from pymongo import MongoClient, ASCENDING
from config import  mongo_host, mongo_password, mongo_user

window_size = 30


class DevTimeSeriesBuilder:
    def __init__(self):
        self.mongo_client = MongoClient("mongodb://{}:27017/".format(mongo_host),
                                username=mongo_user,
                                password=mongo_password)
        self.users_locations = self.get_stored_locations()
        self.streams_per_country = self.get_to_process()


    def get_stored_locations(self):
        users_locations = {}

        for x in self.mongo_client.location_data.clean_users.find({}, projection={"_id": False}):
            users_locations = {**users_locations, **x}

        return users_locations


    def get_to_process(self):
        streams = []
        with open("all_streams.json", "r") as f:
            for l in f:
                streams.append(l.strip())

        streams_per_country = {}
        stream_to_user = {}

        for stream in streams:
            for data in self.mongo_client.entire_streams.latency.find({"stream_id": stream}, projection={"_id": False}): 
                stream_to_user[data["stream_id"]] = data["user_id"]
                
                if data["user_id"] not in self.users_locations or "country_code" not in self.users_locations[data["user_id"]]:
                    continue
                
                if self.users_locations[data["user_id"]]["country_code"] not in streams_per_country:
                    streams_per_country[self.users_locations[data["user_id"]]["country_code"]] = []

                streams_per_country[self.users_locations[data["user_id"]]["country_code"]].append(data["stream_id"])
                break

        return streams_per_country
    
    def remove_missing_digits(self, latency):
        points_to_skip = set()
        
        for idx in range(1, len(latency)-1):
            if len(str(latency[idx]["latency"])) < min(len(str(latency[idx-1]["latency"])), len(str(latency[idx+1]["latency"]))):
                # This is a suspicious point as it has less digits than its neighbours
                if str(latency[idx]["latency"]) in str(latency[idx-1]) and str(latency[idx]["latency"]) in str(latency[idx+1]["latency"]):
                    # skip this point
                    points_to_skip.add(idx)

        return [x for idx, x in enumerate(latency) if idx not in points_to_skip]


    def get_stream_deviation(self, stream_id):
        stream_latency = []
        user_id = None
        game_id = None

        # Pending question: include or remove the points labeled as spikes?
        for x in self.mongo_client.entire_streams_20.clean_latency_entire.find({"stream_id": stream_id}, projection={"_id": False}):
            # if not x.get("is_spike", False):
            stream_latency.append({"latency": int(x["latency"]), "date": datetime.fromtimestamp(int(x["date"]))})
            user_id = x["user_id"]
            game_id = x["game_id"]

        if not stream_latency:
            return []

        # Glitches are obvious now and have a huge influence on the result -> check if you missed a digit
        stream_latency = self.remove_missing_digits(stream_latency)

        df = pd.DataFrame(stream_latency)
        df.set_index('date', inplace= True)
        df.sort_index(inplace=True)
        
        df = df.rolling(window="{}s".format(window_size)).std().dropna()

        deviation_series = []
        for index, row in df.iterrows():
            deviation_series.append({"game_id": game_id, "user_id": user_id, "stream_id": stream_id, "date": index.to_pydatetime().timestamp(), "dev": row["latency"]})
                
        return deviation_series
    
    
    def clean_old(self):
        self.mongo_client.entire_streams_20.deviation_entire_30.drop()


    def run(self):
        for country, stream_list in self.streams_per_country.items():
            print(country)
            for stream_id in tqdm(stream_list):
                dev_series = self.get_stream_deviation(stream_id)

                if dev_series:
                    self.mongo_client.entire_streams_20.deviation_entire_30.insert_many(dev_series)



if __name__ == "__main__":
    builder = DevTimeSeriesBuilder()
    builder.run()