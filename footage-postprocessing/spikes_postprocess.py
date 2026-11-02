from pymongo import MongoClient
from config import mongo_host, mongo_user, mongo_password, stable_period_min_length, stable_share
from datetime import datetime, timedelta
from copy import deepcopy
from tqdm import tqdm

from utils import get_stream_collections

min_points_in_stable = 6
default_qoe_band = 20


class SpikesPostProcessor:
    def __init__(self):
        self.mongo_client = MongoClient("mongodb://{}:27017/".format(mongo_host),
                                        username=mongo_user,
                                        password=mongo_password)


    # Checks the min distance between points to distinguish data that was sampled every second
    @staticmethod
    def classify_sampling(latency):
        min_distance = 24*60*60 

        for idx in range(1, len(latency)):
            min_distance = min(min_distance, (latency[idx]["date"]-latency[idx-1]["date"]).seconds)

        return min_distance

    # Separate the data into observation windows (created from footage samples)
    @staticmethod
    def separate_observation_windows(latency):
        windows = [[latency[0]]]

        for idx in range(1, len(latency)):
            if latency[idx]["date"] - windows[-1][0]["date"] > timedelta(minutes=4, seconds=50):
                windows.append([latency[idx]])
            else:
                windows[-1].append(latency[idx])
        
        return windows


    @staticmethod
    def get_sequence_qoe_groups(latency_sequence, qoe_band=default_qoe_band):        
        qoe_sequence = []

        if not latency_sequence:
            return qoe_sequence

        idx = 0
        while idx < len(latency_sequence) and (int(latency_sequence[idx]["latency"]) == 0 or int(latency_sequence[idx]["latency"]) >= 1000):           
            idx += 1

        if idx == len(latency_sequence):
            return qoe_sequence

        current_sequence = [latency_sequence[idx]]    
        min_sequence = latency_sequence[idx]["latency"]
        max_sequence = latency_sequence[idx]["latency"]
        idx += 1

        for latency in latency_sequence[idx:]:
            if int(latency["latency"]) == 0:
                continue

            if int(latency["latency"]) >= 1000:
                continue

            if (abs(latency["latency"] - min_sequence) <= qoe_band and abs(latency["latency"] - max_sequence) <= qoe_band) and \
                (len(str(min_sequence)) == len(str(latency["latency"])) or len(str(min_sequence)) > 2 or len(str(latency["latency"])) > 2):
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


    # Recompute the stable sequences from the sequence of points inside the same qoe band
    @staticmethod
    def get_periods_from_sequences(game_id, qoe_sequences):
        new_periods = []
        total_points = 0
        total_per_digit_number = {}

        for idx, sequence in enumerate(qoe_sequences):
            latency = [x["latency"] for x in sequence]
            new_points = [x for x in sequence if "old" not in x]
            
            total_points += len(new_points)

            p = {"seq_idx": int(idx), "min": min(latency), "max": max(latency), "start": sequence[0]["date"], "end": sequence[-1]["date"], "raw_period": sequence}
            
            new_periods.append(p)

            if str(len(str(p["max"]))) not in total_per_digit_number:
                total_per_digit_number[str(len(str(p["max"])))] = 0

            total_per_digit_number[str(len(str(p["max"])))] += len(new_points)

        for sequence in new_periods:
            is_stable = timedelta(minutes=stable_period_min_length.get(game_id, 30)) < sequence["end"] - sequence["start"] and \
                        total_per_digit_number[str(len(str(sequence["max"])))]/total_points > stable_share

            # Check if the sequence has enough points to be considered stable anyway (exploration)            
            if not is_stable:
                is_stable = timedelta(minutes=stable_period_min_length.get(game_id, 30)) > sequence["end"] - sequence["start"] and len(sequence["raw_period"]) >= min_points_in_stable

            sequence["stable"] = is_stable
            sequence["start"] = sequence["start"].timestamp()
            sequence["end"] = sequence["end"].timestamp()

        return new_periods


    def clean_spike(self, spike):
        return {**spike["spike"], "date": spike["spike"]["date"].timestamp()}


    def insert_analyzed_spikes(self, stream_id, stream_data):
        real_clean = [self.clean_spike(s) for s in stream_data["real"]]
        fake_clean = [self.clean_spike(s) for s in stream_data["fake"]]
        unconfirmed_clean = [self.clean_spike(s) for s in stream_data["unconfirmed"]]

        self.mongo_client.footage.spikes_analysis.find_one_and_replace({"stream_id": stream_id}, {"stream_id": stream_id, "real": real_clean, "fake": fake_clean, "unconfirmed": unconfirmed_clean}, upsert=True)
        

    def run(self):
        # Get all recently finished spikes (once studied, they need to be moved) -> if a new spike for a stream appears, we can reprocess the entire
        # stream (just be careful) 
        finished_spikes_streams = set([x["stream_id"] for x in self.mongo_client.footage.finished_spikes.find(projection={"_id": False})])

        footage_latency_db = self.mongo_client.footage_data
        thumbnail_latency_db = self.mongo_client.thumbnail_data

        # Only look at streams that supposedly have spikes
        for stream_id in tqdm(finished_spikes_streams):
            stream_latency = []

            stream_metadata = self.mongo_client.thumbnails.stream_ends.find_one({"stream_id": stream_id}, projection={"_id": False})
            if not stream_metadata:
                continue

            collections_to_check = get_stream_collections(stream_metadata)

            for col in collections_to_check:
                stream_latency.extend([{**x, "date": int(x["date"]), "latency": int(x["latency"])} for x in thumbnail_latency_db[col].find({"stream_id": stream_id}, projection={"_id": False}) if "latency" in x and "should_skip" not in x])
    
            for col in collections_to_check:
                for p in footage_latency_db[col].find({"stream_id": stream_id}, projection={"_id": False}):
                    if "latency" in p and "should_skip" not in p:
                        if "values" in p:
                            p.pop("values")

                        if "token" in p:
                            p.pop("token")

                        p["footage"] = True
                        stream_latency.append({**p, "date": int(p["date"]), "latency": int(p["latency"])})

            if not stream_latency:
                continue

            spikes_result = {"real": [], "fake": [], "unconfirmed": []}
            latency = sorted([{**x, "latency": int(x["latency"]), "date": datetime.fromtimestamp(int(x["date"]))} for x in stream_latency], key=lambda x: x["date"])
            
            # If this stream does not have at least one observation window, skip it
            # If this stream does not have any points marked as spikes, skip it (they can be re-classified after the stream ended)

            # Group the data into windows: each window contains all data that is not separated longer than 15 seconds (because of the 30 second
            # rule around the spikes)
            windows = self.separate_observation_windows(latency)
            
            for window in windows:
                # If the window only contains one point or only has thumbnail data (), skip it
                if len(window) == 1 or len([x for x in window if "footage" in x]) == 0:
                    continue

                # Separate all points marked as spikes
                spikes = [{"idx": idx, "spike": x} for idx, x in enumerate(window) if "is_spike" in x]

                # No spike in this window, so just continue
                if len(spikes) == 0:
                    continue
                
                for spike in spikes:
                    previous = []       
                    spike_idx = spike["idx"]             
                    while spike_idx > 0 and spike["spike"]["date"] - window[spike_idx]["date"] < timedelta(minutes=5):
                        previous.append(window[spike_idx])
                        spike_idx -= 1

                    next = []
                    spike_idx = spike["idx"]
                    while spike_idx < len(window)-1 and window[spike_idx]["date"] - spike["spike"]["date"] < timedelta(minutes=5):
                        next.append(window[spike_idx])
                        spike_idx += 1

                    spike["previous"] = previous
                    spike["next"] = next
       
                    # Spikes without close neighbours (within the same window) can not be studied
                    if not spike.get("previous", []) and not spike.get("next", []):
                        spikes_result["unconfirmed"].append(spike)
                    else:
                        # Spikes with at least one close neighbour can be classified two ways: locally real or to further check -> check beyond the direct neighbor 
                        if spike.get("previous", None):
                            found_previous = False
                            for previous in spike["previous"]:
                                if not found_previous:
                                    if abs(spike["spike"]["latency"] - previous["latency"]) > default_qoe_band/2:
                                        spikes_result["real"].append(spike)
                                        found_previous = True

                            if found_previous:       
                                continue
                        
                        if spike.get("next", None):
                            found_next = False
                            for next in spike["next"]:
                                if not found_next:
                                    if abs(spike["spike"]["latency"] - next["latency"]) > default_qoe_band/2:
                                        spikes_result["real"].append(spike)
                                        found_next = True
                            if found_next: 
                                continue

                        spikes_result["fake"].append(spike)
                        
            # Once the spikes have been processed, move them to a different collection: pending, proper cleaning
            if spikes_result["real"] or spikes_result["fake"] or spikes_result["unconfirmed"]:
                self.insert_analyzed_spikes(stream_id, spikes_result)
                

    def clean_old(self):
        self.mongo_client.footage.spikes_analysis.drop()
        

if __name__ == "__main__":
    postprocess = SpikesPostProcessor()
    postprocess.clean_old()
    postprocess.run()

