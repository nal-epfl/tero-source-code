import re
import json
import copy

from datetime import datetime
from game_processors import *

game_id = "902129"

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
            "819896": WutheringWavesProcessor(img_type),
            "902129": MarvelRivalsProcessor(img_type),
            "853669": ArknightsEndfieldProcessor(img_type),

            "21779": LeagueOfLegendsProcessor(img_type),
            "29595": Dota2Processor(img_type),
            "513181": GenshinImpactProcessor(img_type),
            "513143": TeamfightTacticsProcessor(img_type),
            "493057": PubgProcessor(img_type),
            "512710": CallOfDutyProcessor(img_type),
            "1614555304": CallOfDutyProcessor(img_type),
            "510218": AmongUsProcessor(img_type),
            "490100": LostArkProcessor(img_type),
            "511224": ApexLegendsProcessor(img_type),
            "213930085": HonkaiStarRailProcessor(img_type),
            "27546": WorldOfTanks(img_type),
            "32502": WorldOfWarships(img_type),
            "506416": HaloInfinite(img_type),
            "515025": Overwatch2(img_type),
            "460630": Rainbow6(img_type),
            "66366": WarThunder(img_type),
            "490377": SeaOfThieves(img_type),
            "500188": HuntShowdown(img_type),
            "209738946": WutheringWavesProcessor(img_type),
            "1264310518": MarvelRivalsProcessor(img_type),
            "1448964995": ArknightsEndfieldProcessor(img_type)
        }
        self.token_values = {}

    def parse_single_match(self, match):
        pass


    def parse_line(self, line):
        pass


    def accumulate(self, image_info, values, include_area=True):
        # Add special case
        if "token" in image_info:
            if image_info["game_id"] not in self.token_values:
                self.token_values[image_info["game_id"]] = {}

            if image_info["token"] not in self.token_values[image_info["game_id"]]:
                self.token_values[image_info["game_id"]][image_info["token"]] = {}

            self.token_values[image_info["game_id"]][image_info["token"]][image_info["idx"]] = values
            return

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
        m = re.search(r"(?P<game>\d+)_(?P<date>\d\d\d\d-\d\d-\d\d-\d\d-\d\d-\d\d)_(?P<stream_id>\w+)_(?P<user_id>\w+)_area(?P<area>\d+)_bw_(?P<resolution>\d+)", image)
        if m:
            return {"game_id": m.group("game"), "date": m.group("date"), "stream_id": m.group("stream_id"), "user_id": m.group("user_id"), "area_id": int(m.group("area")), "resolution": int(m.group("resolution"))}

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

        m = re.search(r"(?P<game_id>\d+)_(?P<user_id>\w+)_(?P<stream_id>\w+)_(?P<date>\d+)_area(?P<area>\d+)_bw_(?P<resolution>\d+)", image)
        if m:
            return {"game_id": m.group("game_id"), "user_id": m.group("user_id"), "stream_id": m.group("stream_id"), "date": int(m.group("date")), "area_id": int(m.group("area")), "resolution": int(m.group("resolution"))}

        m = re.search(r"(?P<game_id>\d+)_(?P<token>[A-Za-z0-9-]+)_(?P<image_idx>\d+)_area(?P<area>\d+)_bw_(?P<resolution>\d+)", image)       
        if m:
            return {"game_id": m.group("game_id"), "token": m.group("token"), "idx": m.group("image_idx"), "area_id": int(m.group("area")), "resolution": int(m.group("resolution"))} 

        # Add flexible token-based format: only if image does not fit in any of the previous formats.
        m = re.search(r"(?P<game_id>\d+)_(?P<token>[A-Za-z0-9-]+)_(?P<image_idx>\d+)", image)       
        if m:
            return {"game_id": m.group("game_id"), "token": m.group("token"), "idx": m.group("image_idx")} 
        
        # m = re.search(r"(?P<game_id>\d+)_(?P<user_id>[A-Za-z0-9-]+)_(?P<stream_id>[A-Za-z0-9-]+)_(?P<image_idx>\d+)", image)       
        # if m:
        #     return {"game_id": m.group("game_id"), "user_id": m.group("user_id"), "stream_id": m.group("stream_id"), "date": int(m.group("image_idx")), "area_id": 0} 

        m = re.search(r"(?P<date>\d+\.\d+)", image)       
        if m:
            return {"date": float(m.group("date")), "game_id": game_id, "user_id": "a", "stream_id": "b", "area_id": 0} 



    def add_to_data_keys(self, image_info, all_data_keys):
        if image_info["game_id"] not in all_data_keys:
            all_data_keys[image_info["game_id"]] = {}

        if image_info["user_id"] not in all_data_keys[image_info["game_id"]]:
            all_data_keys[image_info["game_id"]][image_info["user_id"]] = {}
            
        if image_info["stream_id"] not in all_data_keys[image_info["game_id"]][image_info["user_id"]]:
            all_data_keys[image_info["game_id"]][image_info["user_id"]][image_info["stream_id"]] = set()
        
        if isinstance(image_info["date"], str):
            all_data_keys[image_info["game_id"]][image_info["user_id"]][image_info["stream_id"]].add(datetime.strptime(image_info["date"], "%Y-%m-%d-%H-%M-%S").timestamp())
        else:
            all_data_keys[image_info["game_id"]][image_info["user_id"]][image_info["stream_id"]].add(image_info["date"])


    def add_to_token_keys(self, image_info, all_token_keys):
        if image_info["game_id"] not in all_token_keys:
            all_token_keys[image_info["game_id"]] = {}

        if image_info["token"] not in all_token_keys[image_info["game_id"]]:
            all_token_keys[image_info["game_id"]][image_info["token"]] = set()

        all_token_keys[image_info["game_id"]][image_info["token"]].add(image_info["idx"])


    def parse_file(self, file_content, all_data_keys, all_token_keys):
        for l in file_content:
            if not l.strip():
                continue

            try:
                results = json.loads(l.strip())
            except Exception:
                continue

            image_info = self.parse_image_name(results["image"])
            try:
                processor = self.processors.get(image_info["game_id"])
            except Exception:
                processor = self.processors.get(game_id)
                
            if not processor:
                continue

            if ("area_id" in image_info and image_info["area_id"] == 0) or ("id" in image_info):
                processor.images_processed += 1

            if "token" in image_info:
                processor.token_processed += 1

            matches = self.parse_line(results)
            matches = sorted(matches, key=lambda x: x[1]["x1"])
            values = processor.get_values(matches, image_info)

            self.accumulate(image_info, values)

            if "token" in image_info:
                self.add_to_token_keys(image_info, all_token_keys)
            else:
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
        # Add case for token-based images
        if "user_id" in info:
            return self.values.get(info["game_id"], {}).get(info["user_id"], {}).get(info["stream_id"], {}).get(info["date"], {})
        
        if "token" in info:
            return self.token_values.get(info["game_id"], {}).get(info["token"], {}).get(info["idx"], {})
           

    def report_processed(self):
        processed = {}

        for game_id, processor in self.processors.items():
            processed[game_id] = processor.images_processed
        
        return processed
    

    def report_token_processed(self):
        processed = {}

        for game_id, processor in self.processors.items():
            processed[game_id] = processor.token_processed
        
        return processed