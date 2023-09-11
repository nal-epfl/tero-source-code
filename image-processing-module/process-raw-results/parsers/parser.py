import re
import json

from datetime import datetime
from game_processors import *

game_id = ""

class Parser:
    def __init__(self, img_type):
        self.values = {}
        self.processors = {
            "295590": LeagueOfLegendsProcessor(img_type),
            "116088": Dota2Processor(img_type),
            "135305": GenshinImpactProcessor(img_type),
            "118849": TeamfightTacticsProcessor(img_type),
            "273195": PubgProcessor(img_type),
            "464426": CallOfDutyProcessor(img_type),
            "273486": CallOfDutyProcessor(img_type),
            "319965": AmongUsProcessor(img_type),
            "747108": LostArkProcessor(img_type),
            "267128": ApexLegendsProcessor(img_type),
            "314852": HonkaiStarRailProcessor(img_type),            
            "742409": WorldOfTanks(img_type),
            "461764": WorldOfWarships(img_type),
            "614266": HaloInfinite(img_type),
            "128974": Overwatch2(img_type),
            "762836": Rainbow6(img_type),
            "970338": WarThunder(img_type),
            "101342": SeaOfThieves(img_type),
            "452439": HuntShowdown(img_type),
        }


    def parse_single_match(self, match):
        pass


    def parse_line(self, line):
        pass


    def accumulate(self, image_info, values, include_area=True):
        if image_info["game_id"] not in self.values:
            self.values[image_info["game_id"]] = {}

        if image_info["user_id"] not in self.values[image_info["game_id"]]:
            self.values[image_info["game_id"]][image_info["user_id"]] = {}
        
        if image_info["stream_id"] not in self.values[image_info["game_id"]][image_info["user_id"]]:
            self.values[image_info["game_id"]][image_info["user_id"]][image_info["stream_id"]] = {}
        
        if image_info["date"] not in self.values[image_info["game_id"]][image_info["user_id"]][image_info["stream_id"]]:
            self.values[image_info["game_id"]][image_info["user_id"]][image_info["stream_id"]][image_info["date"]] = {}

        if include_area:
            self.values[image_info["game_id"]][image_info["user_id"]][image_info["stream_id"]][image_info["date"]][str(image_info["area_id"])] = values
        else:
            self.values[image_info["game_id"]][image_info["user_id"]][image_info["stream_id"]][image_info["date"]] = values


    def parse_image_name(self, image):
        m = re.search(r"(?P<game>\d+)_(?P<date>\d\d\d\d-\d\d-\d\d-\d\d-\d\d-\d\d)_(?P<stream_id>\w+)_(?P<user_id>\w+)_area(?P<area>\d+)", image)
        if m:
            return {"game_id": m.group("game"), "date": m.group("date"), "stream_id": m.group("stream_id"), "user_id": m.group("user_id"), "area_id": int(m.group("area"))}
        
        m = re.search(r"(?P<game>\d+)_(?P<date>\d\d\d\d-\d\d-\d\d-\d\d-\d\d-\d\d)_(?P<stream_id>\w+)_(?P<user_id>\w+)_bw", image)
        if m:
            return {"game_id": m.group("game"), "date": m.group("date"), "stream_id": m.group("stream_id"), "user_id": m.group("user_id"), "area_id": 0}
        
        m = re.search(r"(?P<game>\d+)_(?P<date>\d\d\d\d-\d\d-\d\d-\d\d-\d\d-\d\d)_(?P<stream_id>\w+)_(?P<user_id>\w+)", image)
        if m:
            return {"game_id": m.group("game"), "date": m.group("date"), "stream_id": m.group("stream_id"), "user_id": m.group("user_id"), "area_id": 0}

        m = re.search(r"(?P<date>\d\d\d\d-\d\d-\d\d-\d\d-\d\d-\d\d)_(?P<stream_id>\w+)_(?P<user_id>\w+)_area(?P<area>\d+)", image)
        if m:
            return {"game_id": game_id, "date": m.group("date"), "stream_id": m.group("stream_id"), "user_id": m.group("user_id"), "area_id": int(m.group("area"))}


    def add_to_data_keys(self, image_info, all_data_keys):
        if image_info["game_id"] not in all_data_keys:
            all_data_keys[image_info["game_id"]] = {}

        if image_info["user_id"] not in all_data_keys[image_info["game_id"]]:
            all_data_keys[image_info["game_id"]][image_info["user_id"]] = {}
            
        if image_info["stream_id"] not in all_data_keys[image_info["game_id"]][image_info["user_id"]]:
            all_data_keys[image_info["game_id"]][image_info["user_id"]][image_info["stream_id"]] = set()
        
        all_data_keys[image_info["game_id"]][image_info["user_id"]][image_info["stream_id"]].add(datetime.strptime(image_info["date"], "%Y-%m-%d-%H-%M-%S").timestamp())


    def parse_file(self, file_content, all_data_keys):
        for l in file_content:
            if not l.strip():
                continue

            results = json.loads(l.strip())

            image_info = self.parse_image_name(results["image"]from pymongo import MongoClient
            if image_info["area_id"] == 0:
                processor.images_processed += 1

            matches = self.parse_line(results)
            matches = sorted(matches, key=lambda x: x[1]["x1"])
            values = processor.get_values(matches)

            self.accumulate(image_info, values)
            self.add_to_data_keys(image_info, all_data_keys)


    def post_process(self):
        preliminary_values = copy.deepcopy(self.values)
        self.values = {}

        for game_id, values in preliminary_values.items():
            processor = self.processors.get(game_id)

            if not processor:
                continue
            
            for user_id, streams in values.items():
                for stream_id, dates in streams.items():
                    for date, areas in dates.items():
                        try:
                            parsed_date = datetime.strptime(date, "%Y-%m-%d-%H-%M-%S").timestamp()
                        except Exception:
                            parsed_date = date

                        info = {"game_id": game_id, "user_id": user_id, "stream_id": stream_id, "date": parsed_date}
                        self.accumulate(info, processor.process_areas(areas), include_area=False)


    def get_value(self, info):
        return self.values.get(info["game_id"], {}).get(info["user_id"], {}).get(info["stream_id"], {}).get(info["date"], {})
    

    def report_processed(self):
        processed = {}

        for game_id, processor in self.processors.items():
            processed[game_id] = processor.images_processed
        
        return processed