import redis
import os
import re
import json

from config import redis_host, redis_password, tiny_to_process_path, base_path, tiny_results_storage
from logger import get_logger


techniques_map = {
    "pytesseract": "tesseract_confirm",
    "easyocr": "easyocr_confirm",
    "paddleocr": "paddleocr_confirm"
}


def parse_file_name(file_name):
        m = re.search(r"(?P<date>\d\d\d\d-\d\d-\d\d-\d\d-\d\d-\d\d)_confirmation-(?P<technique>\w+)", file_name)
        if m:
            return {"date": m.group("date"), "technique": techniques_map.get(m.group("technique"), m.group("technique"))}


if __name__ == "__main__":
    logger = get_logger("add_missing")

    logger.info("Starting process...")
    cache = redis.Redis(host=redis_host, port=6379, password=redis_password)

    to_process_lists = [os.path.join(tiny_to_process_path, x) for x in os.listdir(tiny_to_process_path)]
    result_logs = [parse_file_name(x) for x in os.listdir(tiny_results_storage) if "confirmation" in x]

    results_dict = {}

    for to_process in to_process_lists:
        name = to_process.split("/")[-1].split(".")[-2]
        corresponding_results = [r["technique"] for r in result_logs if name in r["date"]]
    
        if len(corresponding_results) != 3:
            results_dict[name] = corresponding_results

    for name, results in results_dict.items():
        missing_results = set(techniques_map.values()) - set(results)

        print(missing_results)

        for missing in missing_results:
            cache.sadd(missing, name)