import pandas as pd
import os
import json

from tqdm import tqdm
from datetime import datetime, timedelta
from statistics import mean, stdev
from sklearn.ensemble import IsolationForest
from pymongo import MongoClient, ASCENDING

from config import  mongo_host, mongo_password, mongo_user

qoe_band = 15

class QoEBandProcess:
    def __init__(self, qoe_band):
        self.qoe_band = qoe_band


    def get_sequence_qoe_groups(self, latency_sequence):        
        qoe_sequence = []
        zeroes = []
        over_1000 = []

        if not latency_sequence:
            return qoe_sequence, zeroes, over_1000

        idx = 0
        while idx < len(latency_sequence) and (int(latency_sequence[idx]["latency"]) == 0 or int(latency_sequence[idx]["latency"]) >= 1000):
            if int(latency_sequence[idx]["latency"]) == 0:
                zeroes.append(latency_sequence[idx])
            elif int(latency_sequence[idx]["latency"]) >= 1000:
                over_1000.append(latency_sequence[idx])
            
            idx += 1

        if idx == len(latency_sequence):
            return qoe_sequence, zeroes, over_1000

        current_sequence = [latency_sequence[idx]]    
        min_sequence = latency_sequence[idx]["latency"]
        max_sequence = latency_sequence[idx]["latency"]
        idx += 1

        for latency in latency_sequence[idx:]:
            if int(latency["latency"]) == 0:
                zeroes.append(latency)
                continue

            if int(latency["latency"]) >= 1000:
                over_1000.append(latency)
                continue

            if (abs(latency["latency"] - min_sequence) <= self.qoe_band and abs(latency["latency"] - max_sequence) <= self.qoe_band):
                current_sequence.append(latency)
                min_sequence = min(min_sequence, latency["latency"])
                max_sequence = max(max_sequence, latency["latency"])
            else:
                qoe_sequence.append(current_sequence)
                min_sequence = latency["latency"]
                max_sequence = latency["latency"]
                current_sequence = [latency]

        if current_sequence:
            qoe_sequence.append(current_sequence)
               
        return qoe_sequence
    

def get_periods_from_sequences(qoe_sequences):
    new_periods = []
       
    for sequence in qoe_sequences:
        if not sequence:
            continue

        latency = [x["latency"] for x in sequence]
            
        p = {"min": min(latency), "max": max(latency), "start": sequence[0]["date"], "end": sequence[-1]["date"], "raw": sequence}
        new_periods.append(p)

    return new_periods


# Parameters to check later
min_difference = 15
K = 1.5


class SpikeDetection:
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


    @staticmethod
    def iqr_bounds(scores,k=1.5):
        q1 = scores.quantile(0.25)
        q3 = scores.quantile(0.75)
        iqr = q3 - q1
        lower_bound = (q1 - k * iqr)
        upper_bound = (q3 + k * iqr)
        
        return lower_bound,upper_bound


    def get_events_iforest(self, K, data):
        user_latency = {}
        
        for x in sorted(data, key=lambda x: x["date"]):
            user_latency[datetime.fromtimestamp(x["date"])] = x["latency"]

        if not user_latency:
            return {}

        df = pd.DataFrame(pd.Series(user_latency))
        model =  IsolationForest(contamination='auto')
        model.fit(df) 

        df["scores"] = model.decision_function(df)    
        lower_bound, upper_bound = self.iqr_bounds(df['scores'], k=K)

        df['anomaly'] = 0
        df['anomaly'] = (df['scores'] < lower_bound) | (df['scores'] > upper_bound)

        a = df.loc[df['anomaly'], [0]]

        return [{"date": a[0].index[idx].to_pydatetime(), "latency": x} for idx, x in enumerate(a[0])]


    def find_anomalies(self, latency):
        latency = sorted(latency, key=lambda x: x["date"])
        anomalies = self.get_events_iforest(K, latency)
        
        anomaly_dates = set([x["date"] for x in anomalies])
        not_anomaly_latency = [x["latency"] for x in latency if x["date"] not in anomaly_dates]
        
        session_spikes = []
        session_glitches = []
        if not_anomaly_latency:
            mean_normal = mean(not_anomaly_latency)

            if len(not_anomaly_latency) > 1:
                standard_dev = stdev([x["latency"] for x in latency if x["date"] not in anomaly_dates])
            else: 
                standard_dev = 0
            
            session_spikes = [x for x in anomalies if x["latency"] >= max(mean_normal + 2*standard_dev, mean_normal + min_difference)]
            session_glitches = [x for x in anomalies if x["latency"] < min(mean_normal - 3*standard_dev, mean_normal - min_difference)]

        return session_spikes, session_glitches


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

                streams_per_country[self.users_locations[data["user_id"]]["country_code"]].append(stream)
                break

        return streams_per_country


    def clean_old(self):
        self.mongo_client.entire_streams.outliers_spikes.drop()
        self.mongo_client.entire_streams.outliers_glitches.drop()


    def run_all(self):
        self.clean_old()

        qoe_band_process = QoEBandProcess(qoe_band)

        for country, country_streams in self.streams_per_country.items():
            print(country)

            for stream_id in tqdm(country_streams):
                # Assume that you already cleaned some of the glitches, and use outlier detection to get rid of the rest
                stream_latency = [{**x, "latency": int(x["latency"])} for x in self.mongo_client.entire_streams.clean_latency_entire.find({"stream_id": stream_id}, projection={"_id": False})]
                spikes, glitches = self.find_anomalies(stream_latency)

                # Some post-processing required for the spikes: 
                # Use the stable band processing: if > 10 sec within 15 seconds, then it is not a spike
                qoe_sequences = qoe_band_process.get_sequence_qoe_groups(spikes)
                periods = get_periods_from_sequences(qoe_sequences)

                spikes_to_store = []
                for period in periods:
                    if period["end"] - period["start"] < timedelta(seconds=10):
                        spikes_to_store.extend(period["raw"])

                if spikes_to_store:
                    self.mongo_client.entire_streams.outliers_spikes.insert_many([{"game_id": stream_latency[0]["game_id"], "user_id": stream_latency[0]["user_id"], 
                                                                                  "stream_id": stream_latency[0]["stream_id"], **x, "date": x["date"].timestamp()} for x in spikes_to_store])

                # All glitches should be stored -> you will need to cross-reference later
                if glitches:
                    self.mongo_client.entire_streams.outliers_glitches.insert_many([{"game_id": stream_latency[0]["game_id"], "user_id": stream_latency[0]["user_id"], 
                                                                                    "stream_id": stream_latency[0]["stream_id"], **x, "date": x["date"].timestamp()} for x in glitches])



if __name__ == "__main__":
    spike_detection = SpikeDetection()
    spike_detection.run_all()