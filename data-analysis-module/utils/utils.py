from collections import Counter


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

            if (abs(latency["latency"] - min_sequence) <= self.qoe_band and abs(latency["latency"] - max_sequence) <= self.qoe_band) and \
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
               
        return qoe_sequence, zeroes, over_1000




def compare_locations(loc1, loc2):
    # Bug fix: I honestly have no idea were the location as lists keep coming back from.
    if isinstance(loc1, list):
        print(loc1)
        loc1 = loc1[0]

    if isinstance(loc2, list):
        print(loc2)
        loc2 = loc2[0]

    if loc1.get("lat", None):
        if loc1["lat"] == loc2["lat"] and loc1["long"] == loc2["long"]:
            return 0

    for key, value in loc1.items():
        #Note: country_code is used instead of country, that's why it's safe to skip it altogether
        if key in ["_id", "country","lat", "long"]:
            continue

        if key not in loc2:
            return 1
        elif loc2[key] != value:            
            return -1

    if len(loc2.keys()) > len(loc1.keys()):
        return 2

    return 0


def get_users_locations(db_controller, users):
    several_locations = []
    users_location = {}

    for user_id in users:
        location = [x for x in db_controller.get_user_location(user_id)]
        if len(location) == 1:           
            users_location[user_id] = location[0]["location"]
        elif len(location) > 1:
            several_locations.append(location)

    for l in several_locations:
        same_location = True
        to_store = l[0]
        
        for idx in range(1, len(l)):
            comparison_result = compare_locations(l[0]["location"], l[idx]["location"]) 

            same_location = same_location and comparison_result >= 0
            if comparison_result == 2:
                to_store = l[idx]

        if same_location:
            users_location[l[0]["user_id"]] = to_store["location"]
    
    return users_location


def get_biggest_division(location):
    if "region" in location:
        return location["region"], "region"
    elif "county" in location:
        return location["county"], "county"
    elif "city" in location:
        return location["city"], "city"

    return None, None


def get_users_by_region(users_locations_by_id, get_locations=False):    
    users_by_region = {}

    for user_id, location in users_locations_by_id.items():
        division, _ = get_biggest_division(location)

        if not division:
            continue

        if location["country_code"] not in users_by_region:
            users_by_region[location["country_code"]] = {}

        if division not in users_by_region[location["country_code"]]:
            users_by_region[location["country_code"]][division] = []

        users_by_region[location["country_code"]][division].append(user_id)
    
    if get_locations:
        return users_by_region, users_locations_by_id
    else:
        return users_by_region


def get_stored_locations(mongo_client):
    users_locations = {}

    for x in mongo_client.processed.locations.find({}, projection={"_id": False}):
        users_locations = {**users_locations, **x}

    return users_locations


def get_alternative_latency(alternative, original):
    latency_alternatives = []

    for x in alternative["values"].values():
        if isinstance(x, dict):
            if "latency" in x:
                latency_alternatives.append(x["latency"])
    
    count = Counter(latency_alternatives).most_common()
    to_return = [int(x[0]) for x in count if int(x[0]) != int(original)]

    if to_return:
        return to_return[0]
    

def group_spike_list(spikes):
    date = spikes[0].pop("date")
    latency_0 = spikes[0]["latency"]
    
    return {**spikes[0], "start": min([date, *[e["date"] for e in spikes[1:]]]), "end": max([date, *[e["date"] for e in spikes[1:]]]), 
            "latency": [{"latency": latency_0, "date": date}, *[{"latency": e["latency"], "date": e["date"]} for e in spikes[1:]]]}



def group_sequences(sequences, qoe_band=15, factor=1):    
    first_seq = None
    for x in sequences:
        first_seq = x
        break
    
    if not first_seq:
        return []

    grouped = [{"min": first_seq["min"], "max": first_seq["max"], "sequences": [first_seq]}]

    for s in sequences[1:]:
        found = False

        for group in grouped:
            if group["min"] <= s["min"] and s["max"] <= group["max"]:
                group["sequences"].append(s)
                found = True
                break
        
        if not found:
            grouped.append({"min": s["min"], "max": s["max"], "sequences": [s]})

    grouped_sequences = [{"min": grouped[0]["min"], "max": grouped[0]["max"], "sequences": grouped[0]["sequences"]}]

    for g in grouped[1:]:
        found = False

        for group in grouped_sequences:
            min_boundary = min(g["min"], group["min"])
            max_boundary = max(g["max"], group["max"])

            if max_boundary - min_boundary <= factor*qoe_band:
                group["min"] = min_boundary
                group["max"] = max_boundary
                group["sequences"].extend(g["sequences"])
                found = True
                break
        
        if not found:
            grouped_sequences.append(g)

    return [{"min": g["min"], "max": g["max"], "sequences": g["sequences"]} for g in grouped_sequences]