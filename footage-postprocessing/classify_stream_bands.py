from datetime import datetime, timedelta
from pymongo import MongoClient
from config import  mongo_host, mongo_password, mongo_user
from logger import get_logger

from utils import get_stream_collections

tolerance = 15/2
bad_band_threshold = 0.1
min_bands_with_samples = 0.5


def get_samples(stream_data):
    footage_latency_db = mongo_client.footage_data

    stream_metadata = mongo_client.thumbnails.stream_ends.find_one({"stream_id": stream_data["stream_id"]}, projection={"_id": False})
    if not stream_metadata:
        return []
    
    collections_to_check = get_stream_collections(stream_metadata)

    all_stream_samples = []
    for col in collections_to_check:
        all_stream_samples.extend([{**x, "date": int(x["date"]), "latency": int(x["latency"])} for x in footage_latency_db[col].find({"stream_id": stream_data["stream_id"]}, projection={"_id": False}) if "latency" in x and "should_skip" not in x])
        
    sampled_by_band = []

    for band in stream_data["bands"]:
        this_band = []
        
        for l in all_stream_samples:
            if band["start"] <= l["date"] and l["date"] <= band["end"]:
                this_band.append(l)

        sampled_by_band.append(this_band)

    return sampled_by_band


def check_if_in_band(band, latency_in_band):
    is_outside = []

    for l in latency_in_band:
        if min(abs(l["latency"] - band["min"]), abs(l["latency"] - band["max"])) > tolerance:
            is_outside.append(l)
            
    return is_outside



if __name__ == "__main__":
    logger = get_logger("classify-bands")
    logger.info("Starting process...")

    mongo_client = MongoClient("mongodb://{}:27017/".format(mongo_host),
                                username=mongo_user,
                                password=mongo_password)

    # It seems hard to make sure that everything has been processed, so just start from scratch every time
    # Empty good/bad collections

    mongo_client.clustering.good_streams.drop()
    mongo_client.clustering.bad_streams.drop()

    bad_streams = []
    good_streams = []
    
    logger.info("Checking all finished streams...")

    streams_inspected = []
    # Get all finished streams -> once you are done with a stream, it needs to disappear from this collection
    for stream_data in mongo_client.footage.finished_streams.find():
        bad_bands = []
        samples = get_samples(stream_data)

        if not samples:
            continue

        # Only proceed if at least half of the bands have samples.
        if len([x for x in samples if x]) / len(samples) < min_bands_with_samples:
            continue

        # Several border cases to handle here:
        # 1. Empty bands -> without confirmation they should be bad, right?
        # 2. At least 10% outside the band (so, if you manage to sample more than one chunk you have some tolerance for spikes).

        for idx in range(0, len(samples)):
            if not samples[idx]:
                continue

            stream_is_bad =  check_if_in_band(stream_data["bands"][idx], samples[idx])

            if len(stream_is_bad) / len(samples[idx]) >= bad_band_threshold:
                bad_bands.append({"band": stream_data["bands"][idx], "outside": stream_is_bad})
        
        # At the end of the band analysis, if the stream has no bad bands, then it is trustwothy
        # Extra condition: it has to have data for at least 50% of the bands -> how much do I lose?
        if not bad_bands:
            good_streams.append(stream_data)
        else:
            bad_streams.append({"bad_bands": bad_bands, "stream_data": stream_data})

    # What to do with the results:
    # 1. Good streams should be stored to be studied: stable bands per country / time of day
    logger.info("Storing stable streams: found {}".format(len(good_streams)))
    for good_stream in good_streams:
        mongo_client.clustering.good_streams.insert_one(good_stream)

    # 2. Bad streams should be stored as backup
    logger.info("Storing bad streams: found {}".format(len(bad_streams)))
    for bad_stream in bad_streams:
        mongo_client.clustering.bad_streams.insert_one(bad_stream)

    logger.info("Finishing process...")

