import redis
import os
import re
import json

from config import redis_host, redis_password, to_process_storage, base_path, results_storage, processing_queues
from logger import get_logger

techniques_map = {
    "pytesseract": "tesseract"
}


def parse_file_name(file_name):
        m = re.search(r"(?P<date>\d\d\d\d-\d\d-\d\d-\d\d-\d\d-\d\d)_matches-(?P<technique>\w+)", file_name)
        if m:
            return {"date": m.group("date"), "technique": techniques_map.get(m.group("technique"), m.group("technique"))}


if __name__ == "__main__":
    logger = get_logger("add_missing")

    logger.info("Starting process...")
    cache = redis.Redis(host=redis_host, port=6379, password=redis_password)

    to_process_lists = [os.path.join(to_process_storage, x) for x in os.listdir(to_process_storage)]
    result_logs = [parse_file_name(x) for x in os.listdir(results_storage) if "matches" in x]

    results_dict = {}

    for to_process in to_process_lists:
        name = to_process.split("/")[-1].split(".")[-2]
        corresponding_results = [r["technique"] for r in result_logs if name in r["date"]]
    
        if len(corresponding_results) != 3:
            results_dict[name] = corresponding_results


    for name, results in results_dict.items():
        if not cache.hexists("currently_processing", name):
            continue
        
        missing_results = set(processing_queues) - set(results)

        for missing in missing_results:
            if not cache.sismember(missing, name):              
                cache.sadd(missing, name)