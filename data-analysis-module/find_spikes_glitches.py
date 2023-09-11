import multiprocessing
import copy

from datetime import datetime, timedelta

from functools import partial
from db.mongo_controller import MongoController
from utils.utils import QoEBandProcess, get_alternative_latency
from utils.logger import get_logger
from config import reference_date, max_spike_proportion

number_cores = 30

# Configurations for bug fixes
warzone_bug_start_date = datetime(year=2023, month=4, day=12).timestamp()
warzone_bug_end_date = datetime(year=2023, month=4, day=12).timestamp()

riot_patch_fix_date = datetime(year=2022, month=7, day=14).timestamp()


class ProcessUserData:
    def __init__(self, user_id, db_controller):          
        self.initial_seq_idx = 0
        self.db_controller = db_controller  

        self.user_id = user_id       
        self.game = None
        self.alternatives = {}
        
    def set_initial_seq_idx(self, initial_idx):
        self.initial_seq_idx = initial_idx
    

    def set_game(self, game):
        self.game = game
        self.qoe_seq = QoEBandProcess(self.db_controller.qoe_band.get(self.game, 15))
        self.alternatives = self.db_controller.get_all_alternatives(self.user_id, self.game)
        self.total_points, self.total_per_digit_number, self.stable = self.db_controller.get_user_game_counts(self.game, self.user_id)
        self.stable_boundaries = self.db_controller.get_user_game_stable_boundaries(self.game, self.user_id)

    

    def get_alternative(self, date):
        return self.alternatives.get(date, None)
        

    def get_all_latency(self):
        latency = {}
        
        for x in self.db_controller.get_all_latency(self.user_id):        
            if x["game_id"] == "464426" and warzone_bug_start_date < x["date"] < warzone_bug_end_date:
                continue

            latency_int = int(x["latency"])
            # Adjustment due to reported bug: https://www.leagueoflegends.com/en-us/news/game-updates/patch-12-13-notes/
            if (x["game_id"] == "295590" or x["game_id"] == "118849") and x["date"] < riot_patch_fix_date:
                latency_int = latency_int - 7 if latency_int > 7 else latency_int

            l = {**x, "date": datetime.fromtimestamp(x["date"]), "latency": latency_int}
            if l["game_id"] not in latency:
                latency[l["game_id"]] = []
            
            latency[l["game_id"]].append(l)
                    
        latency_sorted = {}
        for game, game_latency in latency.items():
            latency_sorted[game] = [{**x, "idx": idx} for idx, x in enumerate(sorted(game_latency, key=lambda x: x['date']))]
            
        return latency_sorted


    def get_latency_game(self, last_sequence_latency, init_idx=0, since=None):
        latency = []
                       
        for x in self.db_controller.get_all_latency(self.user_id, self.game, since):
            if  warzone_bug_start_date < x["date"] < warzone_bug_end_date and self.game == "464426":
                break

            l = int(x["latency"])
            # Adjustment due to reported bug: https://www.leagueoflegends.com/en-us/news/game-updates/patch-12-13-notes/
            if x["date"] < riot_patch_fix_date and (self.game == "295590" or self.game == "118849"):
                l = l - 7 if l > 7 else l
            
            latency.append({**x, "date": datetime.fromtimestamp(x["date"]), "latency": l})


        to_return = []
        for idx, x in enumerate(sorted(latency, key=lambda x: x['date'])):
            new_data = {**x, "idx": int(idx+init_idx)}
            if tuple([x["date"], x["stream_id"], int(idx+init_idx)]) in last_sequence_latency:
                new_data["old"] = True

            to_return.append(new_data)

        return to_return


    def get_periods_from_sequences(self, qoe_sequences, old_spike_periods=set()):
        new_periods = []
        total_points = self.total_points
        total_per_digit_number = self.total_per_digit_number

        for idx, sequence in enumerate(qoe_sequences):
            latency = [x["latency"] for x in sequence]
            new_points = [x for x in sequence if "old" not in x]
            
            total_points += len(new_points)

            p = {"user_id": self.user_id, "game_id": self.game, "seq_idx": int(idx+self.initial_seq_idx), "min": min(latency), "max": max(latency), "start": sequence[0]["date"], "end": sequence[-1]["date"], 
                 "raw_period": sequence}
            
            if tuple([sequence[0]["date"].timestamp(), sequence[-1]["date"].timestamp()]) in old_spike_periods:
                p["is_spike"] = True

            new_periods.append(p)

            if str(len(str(p["max"]))) not in total_per_digit_number:
                total_per_digit_number[str(len(str(p["max"])))] = 0

            total_per_digit_number[str(len(str(p["max"])))] += len(new_points)

        stable = False

        for sequence in new_periods:
            is_stable = timedelta(minutes=self.db_controller.stable_length.get(self.game, 30)) < sequence["end"] - sequence["start"] and \
                        total_per_digit_number[str(len(str(sequence["max"])))]/total_points > self.db_controller.stable_min_share

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

        total_points, total_per_digit_number, _ = self.db_controller.get_user_game_counts(self.game, self.user_id)
        stable_boundaries = self.db_controller.get_user_game_stable_boundaries(self.game, self.user_id)

        latency_to_store = []
        qoe_periods_to_save = []
    
        active_days = set()
        
        for period in periods:
            if period["stable"]:
                stable_boundaries[0] = min(stable_boundaries[0], period["min"])
                stable_boundaries[1] = max(stable_boundaries[1], period["max"])
            
            t = {"game_id": self.game, **period, "length": len(period["raw_period"])}
            
            new_points = [x for x in period["raw_period"] if "old" not in x]

            total_points += len(new_points)
            if str(len(str(period["max"]))) not in total_per_digit_number:
                total_per_digit_number[str(len(str(period["max"])))] = 0

            total_per_digit_number[str(len(str(period["max"])))] += len(new_points)

            for l in period["raw_period"]:
                if l["date"] not in dates_to_ignore:  
                    latency_to_store.append({**l, "latency": int(l["latency"]), "idx": int(l["idx"]), "date": l["date"].timestamp()})
                    active_days.add((l["date"] - reference_date).days) 

            if "raw_period" in t:
                t.pop("raw_period")

            qoe_periods_to_save.append({**t, "start": t["start"].timestamp() if isinstance(t["start"], datetime) else t["start"], "end": t["end"].timestamp() if isinstance(t["end"], datetime) else t["end"]})           
        
        return {"latency": latency_to_store, "qoe": qoe_periods_to_save, "counts": [total_points, total_per_digit_number, stable_boundaries], "active_days": active_days}


    def save_processed(self, periods, to_remove):
        dates_to_ignore = set()

        for tr in to_remove:
            for point in tr["raw_period"]:
                dates_to_ignore.add(point["date"])

        total_points, total_per_digit_number, _ = self.db_controller.get_user_game_counts(self.game, self.user_id)
        stable_boundaries = self.db_controller.get_user_game_stable_boundaries(self.game, self.user_id)

        latency_to_store = []
        qoe_periods_to_save = []
    
        active_days = set()
        
        for period in periods:
            if period["stable"]:
                stable_boundaries[0] = min(stable_boundaries[0], period["min"])
                stable_boundaries[1] = max(stable_boundaries[1], period["max"])
            
            t = {"game_id": self.game, **period, "length": len(period["raw_period"])}
            
            new_points = [x for x in period["raw_period"] if "old" not in x]

            total_points += len(new_points)
            if str(len(str(period["max"]))) not in total_per_digit_number:
                total_per_digit_number[str(len(str(period["max"])))] = 0

            total_per_digit_number[str(len(str(period["max"])))] += len(new_points)

            for l in period["raw_period"]:
                if l["date"] not in dates_to_ignore:  
                    latency_to_store.append({**l, "latency": int(l["latency"]), "idx": int(l["idx"]), "date": l["date"].timestamp()})
                    active_days.add((l["date"] - reference_date).days) 

            if "raw_period" in t:
                t.pop("raw_period")

            qoe_periods_to_save.append({**t, "start": t["start"].timestamp() if isinstance(t["start"], datetime) else t["start"], "end": t["end"].timestamp() if isinstance(t["end"], datetime) else t["end"]})           
        
        return {"latency": latency_to_store, "qoe": qoe_periods_to_save, "counts": [total_points, total_per_digit_number, stable_boundaries], "active_days": active_days}

    
    def clean_up_sequences(self, periods, old_periods, zeroes, over_1000):        
        glitches = []
        
        stable_boundaries = self.stable_boundaries

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
                    
                    previous_stable, next_stable = self.get_stable_neighbour_sequences([*old_periods, *periods], period)

                    if previous_stable:
                        lower_than_neighbours = lower_than_neighbours and previous_stable["min"] - self.db_controller.qoe_band.get(self.game, 15) > period["max"]
                    if next_stable:
                        lower_than_neighbours = lower_than_neighbours and next_stable["min"] - self.db_controller.qoe_band.get(self.game, 15) > period["max"]

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


    def find_spikes(self, periods, old_periods):
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

                        previous_stable, next_stable = self.get_stable_neighbour_sequences([*old_periods, *periods], period)
                        
                        if previous_stable:
                            peak_condition.append(point["latency"] > previous_stable["max"] and abs(point["latency"] - previous_stable["min"]) > self.db_controller.qoe_band.get(self.game, 15))                           
                        
                        if point_idx == 0:
                            comparison_previous = (abs(point["latency"] - previous_stable["min"]) > self.db_controller.qoe_band.get(self.game, 15) if previous_stable else True)
                            
                            if idx > 0:
                                is_spike_condition.append("is_spike" in periods[idx-1] and comparison_previous)
                            elif old_periods:
                                is_spike_condition.append("is_spike" in old_periods[-1] and comparison_previous)
                        else:
                            is_spike_condition.append("is_spike" in period["raw_period"][point_idx-1])

                        if next_stable:
                            peak_condition.append(point["latency"] > next_stable["max"] and abs(point["latency"] - next_stable["min"]) > self.db_controller.qoe_band.get(self.game, 15))

                        if point_idx == len(period["raw_period"]) - 1:
                            comparison_next = (abs(point["latency"] - next_stable["min"]) > self.db_controller.qoe_band.get(self.game, 15) if next_stable else True)

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


    def get_spikes_alternatives(self, periods, old_periods):
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

                        previous_period, next_period = self.get_stable_neighbour_sequences([*old_periods, *periods], period)

                        if previous_period and (previous_period["min"] <= new_latency <= previous_period["max"]) and \
                            (len(str(previous_period["min"])) == len(str(new_latency)) or len(str(previous_period["min"])) > 2 or len(str(new_latency)) > 2):
                            inside_seq = True

                        if not inside_seq:
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


    def check_unstable_sequences(self, periods, old_periods):   
        fixed_glitch = False

        for period in periods:
            if period["stable"] or "is_spike" in period:
                continue

            inside_seq = False
            previous_seq, next_seq = self.get_stable_neighbour_sequences([*old_periods, *periods], period)
            qoe_band = self.db_controller.qoe_band.get(self.game, 15)
            
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


    def remove_unstable_sequences(self, periods, old_periods):   
        to_remove = []
        new_glitches = []

        for period in periods:
            if period["stable"] or "is_spike" in period:
                continue

            inside_seq = False
            previous_seq, next_seq = self.get_stable_neighbour_sequences([*old_periods, *periods], period)
            qoe_band = self.db_controller.qoe_band.get(self.game, 15)
            
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

        
    def number_points(self, periods):
        n_points = 0
        for period in periods:
            n_points += len(period["raw_period"])

        return n_points
    

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


    def process_user_all_games(self):
        latency_per_game = self.get_all_latency()

        for game, latency in latency_per_game.items():
            if game in ["273195", "267128"]:
                continue
            
            self.process_user(game, latency)
                       

    def process_user(self, game, latency=None):
        self.set_game(game)
        
        last_sequence_latency, last_sequence, init_idx = self.db_controller.get_last_sequence_latency(self.game, self.user_id)
        old_periods, old_stable = self.db_controller.get_old_periods(self.game, self.user_id)
            
        if last_sequence:
            self.set_initial_seq_idx(old_periods[-1]["seq_idx"]+1 if old_periods else 0)

        if not latency:
            latency = self.get_latency_game(last_sequence_latency, init_idx=init_idx, since=last_sequence["start"] if last_sequence else None)

        should_reprocess = True
        glitches_to_store = []

        while should_reprocess:
            should_reprocess = False
            qoe_sequence, zeroes, over_1000 = self.qoe_seq.get_sequence_qoe_groups(latency)
            periods, stable = self.get_periods_from_sequences(qoe_sequence)
            
            stable = stable or old_stable

            if stable:
                to_remove, periods, glitches = self.clean_up_sequences(periods, old_periods, zeroes, over_1000)    
            
                spikes = self.find_spikes(periods, old_periods)
                should_reprocess = self.get_spikes_alternatives(periods, old_periods)
                should_reprocess = should_reprocess or self.check_unstable_sequences(periods, old_periods)

                self.db_controller.store_glitches(glitches)
                
                if should_reprocess:
                    latency = self.get_latency_from_periods(periods, list(), remove_spikes=True)
                    print("{} - {}: {} points".format(self.user_id, self.game, len(latency)))
                else:
                    to_remove2, periods, final_glitches = self.remove_unstable_sequences(periods, old_periods)
                    self.db_controller.store_glitches(final_glitches)
                    
        if stable:
            old_spikes = self.db_controller.get_old_spikes(self.game, self.user_id, last_sequence["start"] if last_sequence else None)
            to_save = self.get_to_save(periods, [*to_remove, *to_remove2])
            
            if self.should_save(to_save["qoe"]):
                self.db_controller.store_latency(to_save["latency"])
                self.db_controller.store_qoe_sequences(to_save["qoe"])
                self.db_controller.save_user_game_counts(self.game, self.user_id, *to_save["counts"])
                self.db_controller.store_active_days(self.game, self.user_id, to_save["active_days"])
            
                self.db_controller.store_spikes(spikes, old_spikes)
            else:
                if to_save["latency"]:
                    self.db_controller.store_discarded_latency(to_save["latency"])
                                    
                if spikes:
                    self.db_controller.store_discarded_spikes(spikes)
                     

def process_users(since_date, empty, users):
    reports = {}
    db_controller = MongoController(since_date, empty)

    for user_id in users:
        processor = ProcessUserData(user_id, db_controller)
        reports[user_id] = processor.process_user_all_games()

    return reports


def process_user_games(data):
    db_controller = MongoController()

    for d in data:
        if d["game_id"] in ["273195", "267128"]:
            continue

        db_controller.since_date = d["since"]
        processor = ProcessUserData(d["user_id"], db_controller)
        processor.process_user(d["game_id"])

    return data


class ProcessLatency:
    def __init__(self, empty=False):
        self.logger = get_logger("spikes_glitches_detection")

        self.start_range = datetime(year=2021, month=5, day=24).timestamp()
        self.db_controller = MongoController(self.start_range, empty)
        self.empty = empty


    def get_to_process(self):
        log_entries = self.db_controller.get_log_entries()
        entries_user_game = {}

        for entry in log_entries:
            if tuple([entry["user_id"], entry["game_id"]]) not in entries_user_game:
                entries_user_game[tuple([entry["user_id"], entry["game_id"]])] = []

            entries_user_game[tuple([entry["user_id"], entry["game_id"]])].append(entry["date"])

        return [{"user_id": x[0], "game_id": x[1], "since": min(k)} for x, k in entries_user_game.items()]


    def process_recent(self):
        chunk_size = 100

        to_process = self.get_to_process()

        self.logger.info("Found {} pairs to process".format(len(to_process)))

        users_chunks = [to_process[i:i + chunk_size] for i in range(0, len(to_process), chunk_size)]

        pool = multiprocessing.Pool(number_cores)
        results = pool.imap(func=process_user_games, iterable=users_chunks)

        ready = 0
        for r in results:
            ready += len(r)
            self.logger.info("User finished: {}/{}".format(ready, len(to_process)))


    def process_all_users(self):
        chunk_size = 100
        users = self.db_controller.get_all_users()

        users_chunks = [users[i:i + chunk_size] for i in range(0, len(users), chunk_size)]
        pool = multiprocessing.Pool(number_cores)
        tmp = partial(process_users, self.start_range, self.empty)
        results = pool.imap(func=tmp, iterable=users_chunks)

        ready = 0
        for r in results:
            ready += len(r)
            self.logger.info("Users finished: {}/{}".format(ready, len(users)))
                    
        self.logger.info("Finish processing")


    def index_data(self):
        self.db_controller.mongo_client.processed.active_days.create_index("user_id")
        self.db_controller.mongo_client.processed.active_days.create_index("game_id")

        self.db_controller.mongo_client.processed.user_game_stats.create_index("user_id")
        self.db_controller.mongo_client.processed.user_game_stats.create_index("game_id")
        
        self.db_controller.mongo_client.processed.qoe.create_index("user_id")
        self.db_controller.mongo_client.processed.qoe.create_index("game_id")
        
        self.db_controller.mongo_client.processed.qoe.create_index("user_id")
        self.db_controller.mongo_client.processed.qoe.create_index("game_id")
        self.db_controller.mongo_client.processed.qoe.create_index("start")
        self.db_controller.mongo_client.processed.qoe.create_index("stable")

        self.db_controller.mongo_client.processed.glitches.create_index("user_id")
        self.db_controller.mongo_client.processed.glitches.create_index("game_id")
        self.db_controller.mongo_client.processed.glitches.create_index("date")
        self.db_controller.mongo_client.processed.glitches.create_index("latency")

        self.db_controller.mongo_client.processed.spikes.create_index("user_id")
        self.db_controller.mongo_client.processed.spikes.create_index("game_id")
        self.db_controller.mongo_client.processed.spikes.create_index("date")

        self.db_controller.mongo_client.processed.latency.create_index("user_id")
        self.db_controller.mongo_client.processed.latency.create_index("date")
        self.db_controller.mongo_client.processed.latency.create_index("game_id")
        self.db_controller.mongo_client.processed.latency.create_index("stream_id")
        self.db_controller.mongo_client.processed.latency.create_index("latency")
        self.db_controller.mongo_client.processed.latency.create_index("original_latency")

        self.db_controller.mongo_client.processed.discarded_spikes.create_index("user_id")
        self.db_controller.mongo_client.processed.discarded_spikes.create_index("game_id")
        self.db_controller.mongo_client.processed.discarded_spikes.create_index("date")

        self.db_controller.mongo_client.processed.discarded_latency.create_index("user_id")
        self.db_controller.mongo_client.processed.discarded_latency.create_index("game_id")
        self.db_controller.mongo_client.processed.discarded_latency.create_index("date")


    def clean_all(self):
        self.db_controller.mongo_client.processed.active_days.drop()
        self.db_controller.mongo_client.processed.user_game_stats.drop()
                
        self.db_controller.mongo_client.processed.qoe.drop()
        self.db_controller.mongo_client.processed.glitches.drop()
        self.db_controller.mongo_client.processed.spikes.drop()
        
        self.db_controller.mongo_client.processed.latency.drop()
        
        self.db_controller.mongo_client.processed.discarded_spikes.drop()
        self.db_controller.mongo_client.processed.discarded_latency.drop()

  

def run_complete_pipeline():    
    try:
        processor = ProcessLatency()
        processor.process_recent()
    except Exception as e:
        logger = get_logger("spikes_glitches_detection")
        logger.info("Error while processing: {}".format(e))


if __name__ == "__main__":
    empty = False

    if empty:
        processor = ProcessLatency(empty=True)
        processor.clean_all()
        processor.process_all_users()
        processor.index_data()
    else:
        processor = ProcessLatency()
        processor.process_recent()