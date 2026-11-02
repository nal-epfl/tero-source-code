from datetime import datetime, timedelta

def separate_into_subsequences(mongo_client, game_id, user_id, changes):
    dates = sorted([x["date"] for x in mongo_client.processed.latency.find({"game_id": game_id, "user_id": user_id}, projection={"_id": False})])
    
    start_date = dates[0]
    end_date = dates[-1]

    subsequences = []
    transitions = []
    change = changes[0]

    for idx in range(0, len(changes)):        
        change = changes[idx]
        subsequences.append({"start": start_date, "end": change["date"]["start"], "min": change["groups"][0]["min"], "max": change["groups"][0]["max"], "subseq_idx": idx, "streams": change["inside_stream"]})
        transitions.append({"idxs": [idx, idx+1], "possible_server_change": change["inside_stream"][0] == change["inside_stream"][1]})
        start_date = change["date"]["end"]
        
    subsequences.append({"start": change["date"]["end"], "end": end_date, "min": change["groups"][1]["min"], "max": change["groups"][1]["max"], "subseq_idx": len(changes), "streams": change["inside_stream"]})

    return subsequences, transitions


def group_sequences(sequences, qoe_band, factor=1):    
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


def process_user_changes(mongo_client, game_id, user_id, changes):
    subsequences, transitions = separate_into_subsequences(mongo_client, game_id, user_id, changes)

    grouped_subsequences = group_sequences(subsequences, qoe_band=20, factor=2)
    flattened_subsequences = []
    for idx, group in enumerate(grouped_subsequences):
        for subseq in group["sequences"]:
            flattened_subsequences.append({"group_idx": idx, **subseq})

    flattened_subsequences.sort(key=lambda x: x["subseq_idx"])
    group_transitions = {}
    for idx in range(0, len(flattened_subsequences)-1):
        group_change = tuple(sorted([flattened_subsequences[idx]["group_idx"], flattened_subsequences[idx+1]["group_idx"]]))
        possible_server_change = transitions[idx]["possible_server_change"]

        if group_change not in group_transitions:
            group_transitions[group_change] = {"server_change": False, "streams": []}
        
        group_transitions[group_change]["server_change"] = group_transitions[group_change]["server_change"] or possible_server_change
        group_transitions[group_change]["streams"].append([list(set(flattened_subsequences[idx]["streams"])), [flattened_subsequences[idx]["start"], flattened_subsequences[idx]["end"]]])

    return flattened_subsequences, group_transitions



def get_stream_collections(stream_metadata, tag="latency"):
    starting_date = datetime.fromtimestamp(int(stream_metadata["start"]))

    collections_to_check = ["{}_{}_{:02d}".format(tag, starting_date.year, starting_date.month)]
    while starting_date + timedelta(days=28) < datetime.fromtimestamp(int(stream_metadata["end"])):
        starting_date = starting_date + timedelta(days=28)
        collections_to_check.append("{}_{}_{:02d}".format(tag, starting_date.year, starting_date.month))

    return list(set(collections_to_check))