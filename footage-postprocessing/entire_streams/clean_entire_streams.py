import multiprocessing
import copy
import json

from pymongo import MongoClient
from datetime import datetime, timedelta
from functools import partial
from collections import Counter

from config import mongo_host, mongo_user, mongo_password, qoe_band, stable_period_min_length, stable_share

import os

path = "finished_entire_streams"

max_spike_proportion = .5
number_cores = 30
min_points_in_stable = 10 # Be more strict, 5 minutes? 2.5 minutes? 1 minute?
# Being more strict here means more spikes (most of them fake)
# 10 consecutive points with increased latency are not necessarily spikes


def get_alternative_latency(alternative, original):
    latency_alternatives = []

    if isinstance(alternative["values"], dict):
        for x in alternative["values"].values():
            if isinstance(x, dict):
                if "latency" in x:
                    latency_alternatives.append(x["latency"])
    else:
        latency_alternatives.extend(alternative["values"])
    
    count = Counter(latency_alternatives).most_common()
    to_return = [int(x[0]) for x in count if int(x[0]) != int(original)]

    if to_return:
        return to_return[0]
    

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
                # and (len(str(min_sequence)) == len(str(latency["latency"])) or len(str(min_sequence)) > 2 or len(str(latency["latency"])) > 2):
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
               
        return qoe_sequence, zeroes, over_1000



class CleanStream:
    def __init__(self, mongo_client, stream_id, game_id):          
        self.initial_seq_idx = 0
        self.mongo_client = mongo_client
        self.game_id = game_id
        self.stream_id = stream_id
        self.qoe_seq = QoEBandProcess(qoe_band)
        self.alternatives = self.get_all_alternatives()
    

    def get_all_alternatives(self):
        alternatives = {}
        
        for x in self.mongo_client.data.alternative_values.find({"stream_id": self.stream_id}, projection={"_id": False}):
            alternatives[x["date"]] = x

        for x in self.mongo_client.explore_users.alternative_values.find({"stream_id": self.stream_id}, projection={"_id": False}):
            alternatives[x["date"]] = x
        
        return alternatives
        
    
    def get_alternative(self, date):
        return self.alternatives.get(date, None)
        

    def get_stream_latency(self):        
        return []



    def get_periods_from_sequences(self, qoe_sequences, old_spike_periods=set()):
        new_periods = []
        total_points = 0
        total_per_digit_number = {}

        for idx, sequence in enumerate(qoe_sequences):
            latency = [x["latency"] for x in sequence]
            new_points = [x for x in sequence if "old" not in x]
            
            total_points += len(new_points)

            p = {"seq_idx": int(idx), "min": min(latency), "max": max(latency), "start": sequence[0]["date"], "end": sequence[-1]["date"], "raw_period": sequence}
            
            if tuple([sequence[0]["date"].timestamp(), sequence[-1]["date"].timestamp()]) in old_spike_periods:
                p["is_spike"] = True

            new_periods.append(p)

            if str(len(str(p["max"]))) not in total_per_digit_number:
                total_per_digit_number[str(len(str(p["max"])))] = 0

            total_per_digit_number[str(len(str(p["max"])))] += len(new_points)

        stable = False

        for sequence in new_periods:
            is_stable = timedelta(minutes=stable_period_min_length.get(self.game_id, 30)) < sequence["end"] - sequence["start"] and \
                        total_per_digit_number[str(len(str(sequence["max"])))]/total_points > stable_share

            # Check if the sequence has enough points to be considered stable anyway (exploration)            
            if not is_stable:
                is_stable = timedelta(minutes=stable_period_min_length.get(self.game_id, 30)) > sequence["end"] - sequence["start"] and len(sequence["raw_period"]) >= min_points_in_stable

            sequence["stable"] = is_stable
            sequence["start"] = sequence["start"].timestamp()
            sequence["end"] = sequence["end"].timestamp()

            stable = stable or is_stable

        return new_periods, stable


    def remove_and_stitch(self, periods, keep_spikes=False):
        good_latency = []
        old_spikes = set()
        
        for x in periods:
            if not x.get("to_remove", False):
                good_latency.extend(x["raw_period"])

            if keep_spikes and "is_spike" in x:
                old_spikes.add(tuple([x["start"], x["end"]]))

        if not good_latency:
            return []

        qoe_sequence, _, _ = self.qoe_seq.get_sequence_qoe_groups(good_latency)
        periods, _ = self.get_periods_from_sequences(qoe_sequence, old_spikes)

        return periods
    

    def get_latency_from_periods(self, periods, to_add, remove_spikes=False):
        good_latency = []
        for period in periods:
            if remove_spikes:
                for x in period["raw_period"]:
                    if "is_spike" in x:
                        x.pop("is_spike")
                    good_latency.append(x)
            else:
                good_latency.extend(period["raw_period"])
        
        good_latency.extend(to_add)
        good_latency.sort(key=lambda x: x["date"])

        if not good_latency:
            return []

        return good_latency


    def add_to_periods(self, periods, to_add):
        good_latency = self.get_latency_from_periods(periods, to_add)

        qoe_sequence, _, _ = self.qoe_seq.get_sequence_qoe_groups(good_latency)
        periods, _ = self.get_periods_from_sequences(qoe_sequence)

        return periods


    def get_to_save(self, periods, to_remove):
        dates_to_ignore = set()

        for tr in to_remove:
            for point in tr["raw_period"]:
                dates_to_ignore.add(point["date"])

        latency_to_store = []
        qoe_periods_to_save = []
            
        for period in periods:
            t = {"game_id": self.game_id, **period, "length": len(period["raw_period"])}
        
            for l in period["raw_period"]:
                if l["date"] not in dates_to_ignore:  
                    latency_to_store.append({**l, "latency": int(l["latency"]), "idx": int(l["idx"]), "date": l["date"].timestamp()})
                    
            if "raw_period" in t:
                t.pop("raw_period")

            qoe_periods_to_save.append({**t, "start": t["start"].timestamp() if isinstance(t["start"], datetime) else t["start"], "end": t["end"].timestamp() if isinstance(t["end"], datetime) else t["end"]})           
        
        return {"latency": latency_to_store, "qoe": qoe_periods_to_save}
   

    def clean_up_sequences(self, periods, zeroes, over_1000):        
        glitches = []
        
        stable_boundaries = [1000000, -1]

        for period in periods:
            if period["stable"]:
                stable_boundaries[0] = min(stable_boundaries[0], period["min"])
                stable_boundaries[1] = max(stable_boundaries[1], period["max"])

        # First, try to fix the zeroes: anything that is not a zero will be tried out. If there is no alternative, the zero IS a glitch.
        to_add_to_periods = []
        
        for zero in [*zeroes, *over_1000]:
            alternative = self.get_alternative(zero["date"].timestamp() if isinstance(zero["date"], datetime) else zero["date"])
            if alternative:
                latency_alternatives = get_alternative_latency(alternative, zero["latency"])
    
                if latency_alternatives:
                    new_latency = int(latency_alternatives)

                    # Mark them as an alternative, if they are glitches they need to disappear immediately
                    zero["original_latency"] = zero["latency"]
                    zero["latency"] = new_latency
                    to_add_to_periods.append(zero)
            else:
                glitches.append({**zero, "date": zero["date"].timestamp()})

        if to_add_to_periods:
            periods = self.add_to_periods(periods, to_add_to_periods)

        keep_looping = True

        # You keep looping until no more glitches are fixed (that's your final state)
        while keep_looping:
            keep_looping = False
            to_remove = []
            
            for idx, period  in enumerate(periods):
                if not period["stable"]:
                    lower_than_neighbours = True
                    
                    previous_stable, next_stable = self.get_stable_neighbour_sequences(periods, period)

                    if previous_stable:
                        lower_than_neighbours = lower_than_neighbours and previous_stable["min"] - qoe_band > period["max"]
                    if next_stable:
                        lower_than_neighbours = lower_than_neighbours and next_stable["min"] - qoe_band > period["max"]

                    if lower_than_neighbours:              
                        has_been_stable = False

                        if not has_been_stable:    
                            # This is a glitch! Ok, can you fix it somehow? 
                            fixes_available = False
                            
                            for glitch in period["raw_period"]:
                                if "original_latency" in glitch:
                                    new_glitch = copy.deepcopy(glitch)
                                    glitches.append({**new_glitch, "date": new_glitch["date"].timestamp() if isinstance(new_glitch["date"], datetime) else new_glitch["date"]})
                                    continue

                                fixed_glitch = self.check_if_glitch_fixed(glitch, previous_stable, next_stable)

                                if fixed_glitch:
                                    keep_looping = True
                                    fixes_available = True
                                    
                                else:
                                    new_glitch = copy.deepcopy(glitch)
                                    glitches.append({**new_glitch, "date": new_glitch["date"].timestamp() if isinstance(new_glitch["date"], datetime) else new_glitch["date"]})
                                
                            if not fixes_available:    
                                period["to_remove"] = True
                                to_remove.append(period)
            
            if keep_looping:
                periods = self.remove_and_stitch(periods)
         
        periods = self.remove_and_stitch(periods)
        return to_remove, periods, glitches


    def check_if_glitch_fixed(self, glitch, previous_period, next_period):
        fixed_glitch = False

        alternative = self.get_alternative(glitch["date"].timestamp() if isinstance(glitch["date"], datetime) else glitch["date"])
        if alternative:
            latency_alternatives = get_alternative_latency(alternative, glitch["latency"])

            if latency_alternatives:
                new_latency = int(latency_alternatives)
                # There is an alternative value! Does it fix the problem?
                # What you would like is for the glitch to become similar to either of the neighbours 

                if previous_period:
                    fixed_glitch = fixed_glitch or (abs(previous_period["min"] - int(new_latency)) < abs(previous_period["min"] - int(glitch["latency"])))
                if next_period:
                    fixed_glitch = fixed_glitch and (abs(next_period["min"] - int(new_latency)) < abs(next_period["min"] - int(glitch["latency"])))

                if fixed_glitch:
                    glitch["original_latency"] = glitch["latency"]
                    glitch["latency"] = new_latency

                    if "old" in glitch:
                        glitch.pop("old")

        return fixed_glitch


    def find_spikes(self, periods):
        all_spikes = []
        continue_looping = True
        
        while continue_looping:
            continue_looping = False
         
            for idx, period in enumerate(periods):
                if not period["stable"] and "is_spike" not in period:
                    for point_idx, point in enumerate(period["raw_period"]):
                        if "is_spike" in point:
                            continue
                        
                        is_spike_condition = []
                        peak_condition = []

                        previous_stable, next_stable = self.get_stable_neighbour_sequences(periods, period)
                        
                        if previous_stable:
                            peak_condition.append(point["latency"] > previous_stable["max"] and abs(point["latency"] - previous_stable["min"]) > qoe_band)                           
                        
                        if point_idx == 0:
                            comparison_previous = (abs(point["latency"] - previous_stable["min"]) > qoe_band if previous_stable else True)
                            
                            if idx > 0:
                                is_spike_condition.append("is_spike" in periods[idx-1] and comparison_previous)
                        else:
                            is_spike_condition.append("is_spike" in period["raw_period"][point_idx-1])

                        if next_stable:
                            peak_condition.append(point["latency"] > next_stable["max"] and abs(point["latency"] - next_stable["min"]) > qoe_band)

                        if point_idx == len(period["raw_period"]) - 1:
                            comparison_next = (abs(point["latency"] - next_stable["min"]) > qoe_band if next_stable else True)

                            if idx < len(periods) - 1:
                                is_spike_condition.append("is_spike" in periods[idx+1] and comparison_next)
                        else:                              
                            is_spike_condition.append("is_spike" in period["raw_period"][point_idx+1])
                                                   
                        and_condition = True
                        for c in peak_condition:
                            and_condition = and_condition and c
                        
                        or_condition = False
                        for c in peak_condition:
                            or_condition = or_condition or c

                        is_spike = False
                        for c in is_spike_condition:
                            is_spike = is_spike or c

                        if and_condition or (or_condition and is_spike):
                            continue_looping = True                        
                            point["is_spike"] = True

                    period_is_spike = True                        
                    for point in period["raw_period"]:
                        period_is_spike = period_is_spike and "is_spike" in point
                    
                    if period_is_spike:
                        period["is_spike"] = True
                        for spike in period["raw_period"]:
                            if "old" in spike:
                                spike.pop("old")
                            
                            new_spike = copy.deepcopy(spike)
                            all_spikes.append({**new_spike, "date": new_spike["date"].timestamp() if isinstance(new_spike["date"], datetime) else new_spike["date"]})

        return all_spikes


    def get_spikes_alternatives(self, periods):
        should_reprocess = False

        for period in periods:
            if "is_spike" not in period:
                continue

            for spike in period["raw_period"]:
                if "original_latency" in spike:
                    continue

                alternative = self.get_alternative(spike["date"].timestamp())                        
                if alternative:
                    new_latency = get_alternative_latency(alternative, int(spike["latency"]))
                    
                    if new_latency:
                        new_latency = int(new_latency)
                                            
                        inside_seq = False

                        previous_period, next_period = self.get_stable_neighbour_sequences(periods, period)

                        # You can tune the condition here a little to make it more likely to get "realistic" values and not some coincidences. E.g. force the value to be inside the band instead. 
                        # Update: changed. Old version left as a comment just in case                      
                        # if previous_period and (abs(new_latency - previous_period["min"]) <= qoe_band and abs(new_latency - previous_period["max"]) <= qoe_band) and \
                        if previous_period and (previous_period["min"] <= new_latency <= previous_period["max"]) and \
                            (len(str(previous_period["min"])) == len(str(new_latency)) or len(str(previous_period["min"])) > 2 or len(str(new_latency)) > 2):
                            inside_seq = True

                        if not inside_seq:
                            # if next_period and (abs(new_latency - next_period["min"]) <= qoe_band and abs(new_latency - next_period["max"]) <= qoe_band) and \
                            if next_period and (next_period["min"] <= new_latency <= next_period["max"]) and \
                                (len(str(next_period["min"])) == len(str(new_latency)) or len(str(next_period["min"])) > 2 or len(str(new_latency)) > 2):
                                inside_seq = True

                        # We forbid fixes that go from 2 digits to 1 digit, it is too easy to add an extra glitch and we should examine the sequences first
                        if inside_seq and not (len(str(spike["latency"])) == 2 and len(str(new_latency)) == 1):
                            should_reprocess = True
                            spike["original_latency"] = spike["latency"]
                            spike["latency"] = new_latency
                            spike["was_spike"] = True
                            
                            if "old" in spike:
                                spike.pop("old")


        return should_reprocess


    def get_stable_neighbour_sequences(self, periods, period):
        previous_stable = period["seq_idx"]
    
        while previous_stable >= 0 and not periods[previous_stable]["stable"]:
            previous_stable -= 1
        
        next_stable = period["seq_idx"]
        while next_stable < len(periods) and not periods[next_stable]["stable"]:
            next_stable += 1

        previous_period = periods[previous_stable] if previous_stable >= 0 else None
        next_period = periods[next_stable] if next_stable < len(periods) - 1 else None

        return previous_period, next_period


    def check_unstable_sequences(self, periods):   
        fixed_glitch = False

        for period in periods:
            if period["stable"] or "is_spike" in period:
                continue

            inside_seq = False
            previous_seq, next_seq = self.get_stable_neighbour_sequences(periods, period)
                        
            if previous_seq and (abs(period["min"] - previous_seq["min"]) <= qoe_band and abs(period["max"] - previous_seq["max"]) <= qoe_band):
                inside_seq = True

            if not inside_seq:
                if not next_seq or (abs(period["min"] - next_seq["min"]) <= qoe_band and abs(period["max"] - next_seq["max"]) <= qoe_band):
                    inside_seq = True

            if not inside_seq:        
                for point in period["raw_period"]:
                    if "original_latency" in point:                
                        continue

                    fixed_this_glitch = self.check_if_glitch_fixed(point, previous_seq, next_seq)
                    fixed_glitch = fixed_glitch or fixed_this_glitch
           
        return fixed_glitch


    def remove_unstable_sequences(self, periods):   
        to_remove = []
        new_glitches = []

        for period in periods:
            if period["stable"] or "is_spike" in period:
                continue

            inside_seq = False
            previous_seq, next_seq = self.get_stable_neighbour_sequences(periods, period)
                        
            if previous_seq and (abs(period["min"] - previous_seq["min"]) <= qoe_band and abs(period["max"] - previous_seq["max"]) <= qoe_band):
                inside_seq = True

            if not inside_seq:
                if not next_seq or (abs(period["min"] - next_seq["min"]) <= qoe_band and abs(period["max"] - next_seq["max"]) <= qoe_band):
                    inside_seq = True

            if not inside_seq:        
                period["to_remove"] = True
                to_remove.append(period)

                for glitch in period["raw_period"]:
                    new_glitch = copy.deepcopy(glitch)
                    new_glitches.append({**new_glitch, "date": new_glitch["date"].timestamp() if isinstance(new_glitch["date"], datetime) else new_glitch["date"]})
        
        if to_remove:
            periods = self.remove_and_stitch(periods, keep_spikes=True)

        return to_remove, periods, new_glitches
       

    def should_save(self, periods):
        counters = {"stable": 0, "unstable": 0, "spikes": 0}

        for period in periods:
            if period["stable"]:
                counters["stable"] += period["length"]
            elif "is_spike" in period:
                counters["spikes"] += period["length"]
            else:
                counters["unstable"] += period["length"]

        if (counters["stable"] + counters["spikes"] + counters["unstable"]) > 0:
            return counters["spikes"]/(counters["stable"] + counters["spikes"] + counters["unstable"]) < max_spike_proportion
        
        return False


    def process_stream(self, latency_to_clean):
        all_latency = [{**x, "idx": idx} for idx, x in enumerate(sorted([*self.get_stream_latency(), *latency_to_clean], key=lambda x: x['date']))]

        # We add two small pre-processing cleaning steps to remove dropped digits and increased digits glitches
        latency = self.remove_missing_digits(all_latency)
        latency = self.remove_added_digits(latency)

        self.run_process(latency)
    

    def store_latency(self, latency):
        if latency:
            cleaned = []
            for ts in latency:
                if not "to_keep" in ts:
                    continue
                
                ts.pop("to_keep")

                if "values" in ts:
                    ts.pop("values")
                cleaned.append(ts)

            if cleaned:
                self.mongo_client.entire_streams.clean_latency_entire.insert_many(cleaned)

    
    def store_glitches(self, glitches):
        if glitches:
            clean = []

            for g in glitches:
                if "_id" in g:
                    g.pop("_id")
                clean.append(g)
            
            if clean:
                self.mongo_client.entire_streams.glitches_entire.insert_many(clean)


    def store_spikes(self, spikes):
        if spikes:
            clean = []

            for s in spikes:
                if "_id" in s:
                    s.pop("_id")
                clean.append(s)
            
            if clean:
                self.mongo_client.entire_streams.spikes_entire.insert_many(clean)


    def run_process(self, latency):            
        should_reprocess = True

        while should_reprocess:
            should_reprocess = False
            qoe_sequence, zeroes, over_1000 = self.qoe_seq.get_sequence_qoe_groups(latency)
            periods, stable = self.get_periods_from_sequences(qoe_sequence)

            if stable:
                to_remove, periods, glitches = self.clean_up_sequences(periods, zeroes, over_1000)    
            
                spikes = self.find_spikes(periods)
                should_reprocess = self.get_spikes_alternatives(periods)
                should_reprocess = should_reprocess or self.check_unstable_sequences(periods)

                self.store_glitches(glitches)
                
                if should_reprocess:
                    latency = self.get_latency_from_periods(periods, list(), remove_spikes=True)
                else:
                    to_remove2, periods, final_glitches = self.remove_unstable_sequences(periods)
                    self.store_glitches(final_glitches)

        if stable:
            to_save = self.get_to_save(periods, [*to_remove, *to_remove2])
            
            if self.should_save(to_save["qoe"]):
                self.store_latency(to_save["latency"])                
                self.store_spikes(spikes)
                self.store_qoe_sequences(to_save["qoe"], to_save["latency"][0])   


    def store_qoe_sequences(self, to_store, latency_sample):
        if to_store:
            self.mongo_client.entire_streams.qoe_entire.insert_many([{"user_id": latency_sample["user_id"], "stream_id": latency_sample["stream_id"], **ts} for ts in to_store])
       
       

def process_streams(streams):
    mongo_client = MongoClient("mongodb://{}:27017/".format(mongo_host),
                            username=mongo_user,
                            password=mongo_password)

    reports = []
    
    for stream in streams:    
        latency_per_game = {}
        with open("{}/{}.json".format(path, stream), "r") as f:
            for line in f:
                l = json.loads(line.strip())
            
        #         if "latency" in l:
        #             if l["game_id"] not in latency_per_game:
        #                 latency_per_game[l["game_id"]] = []

        #             latency_per_game[l["game_id"]].append({**l, "date": datetime.fromtimestamp(int(l["date"])), "latency": int(l["latency"]), "to_keep": True})
        #             total_counter += 1
        
        for l in mongo_client.entire_streams.latency.find({"stream_id": stream}, projection={"_id": False}):            
            if "latency" in l:
                if l["game_id"] not in latency_per_game:
                    latency_per_game[l["game_id"]] = []

                    latency_per_game[l["game_id"]].append({**l, "date": datetime.fromtimestamp(int(l["date"])), "latency": int(l["latency"]), "to_keep": True})

        for game_id, game_latency in latency_per_game.items():
            processor = CleanStream(mongo_client, stream, game_id)
            processor.process_stream(game_latency)
            
            reports.append({"game_id": game_id, "stream_id": stream, "total": total_counter})

    return reports





class ProcessStreams:
    def __init__(self):
        self.mongo_client = MongoClient("mongodb://{}:27017/".format(mongo_host),
                            username=mongo_user,
                            password=mongo_password)
        

    def get_streams_to_process(self):  
        return [x.replace(".json", "") for x in os.listdir(path)]
        

    def process_streams(self):
        self.clean_old()

        chunk_size = 100
        streams = self.get_streams_to_process()
        
        streams_chunks = [streams[i:i + chunk_size] for i in range(0, len(streams), chunk_size)]
        pool = multiprocessing.Pool(number_cores)
        tmp = partial(process_streams)
        results = pool.imap(func=tmp, iterable=streams_chunks)

        with open("reports.json", "w+") as f:
            ready = 0
            for r in results:
                ready += len(r)
                for i in r:
                    f.write("{}\n".format(json.dumps(i)))



if __name__ == "__main__":
    processor = ProcessStreams()
    processor.process_streams()
    

