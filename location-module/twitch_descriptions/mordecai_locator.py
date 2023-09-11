import cld3
import re
import json
import numpy as np

from mordecai import Geoparser
from langdetect import detect, lang_detect_exception

# Instructions: https://github.com/openeventdata/mordecai
# Requirement: python -m spacy download en_core_web_lg
class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)


class MordecaiLocator:
    def __init__(self) -> None:
        self.mordecai_parser = Geoparser()
        self.valid_codes = ["ADM1", "ADM1H", "ADM2", "ADM2H", "ADM3", "ADM3H", "ADM4", "ADM4H", "ADM5", "ADM5H", "PPL", "PPLA", "PPLA2", "PPLA3", 
                "PPLA4", "PPLA5", "PPLC", "PPLCH", "PCLI", "PCLD", "PCLIX"]
        self.characters_to_remove = re.compile("[-|!?;:,.+()0123456789]+")

        self.mordecai_discard_rules = [
            [["bethesda", "broadway", "valhalla", "treasure island", "farmville", "daytona"], [""]],
            [["gisborne"], ["east coast"]],
            [["miami"], ["hotline miami"]],
            [["detroit"], ["human"]],
            [["chicago"], ["remastered"]],
            [["boston"], ["terrier"]],
        ]

        self.mordecai_forced_pairs = [
            ["queens", "new york"],
            ["phoenix", "arizona"]
        ]

        self.mordecai_position_rules = [
            [[], ["name is", "i'm", "im", "can call me", "welcome to", "names", "named", "known as", "called", "name s", "name's"], 1],
            [[], ["sports", "sports."], 0]
        ]


    def parse_mordecai(self, data):
        to_skip = False
        for discard in self.mordecai_discard_rules:
            for x in data["entities"]:
                if x.get("geo", {}).get("place_name", "").lower() in discard[0] and [y for y in discard[1] if y in data["data"]["description"].lower()]:
                    to_skip = True 
        
        for discard in self.mordecai_forced_pairs:
            contains_first = [x for x in data["entities"] if x.get("geo", {}).get("place_name", "").lower() in discard[0]]

            if contains_first:
                if len(set(discard) - set([x.get("geo", {}).get("place_name", "").lower() for x in data["entities"]])) != 0:
                    to_skip = True

        for discard in self.mordecai_position_rules:
            for x in data["entities"]:
                place_name = x.get("geo", {}).get("place_name", "").lower()
                if place_name in discard[0] or (not discard[0]):
                    for tf in discard[1]:
                        if discard[2]:
                            to_find = "{} {}".format(tf, place_name)
                        else:
                            to_find = "{} {}".format(place_name, tf)

                        if to_find.lower() in " ".join([self.characters_to_remove.sub(r'', x) for x in data["data"]["description"].lower().split()]).strip():
                            to_skip = True 
                        
                        if to_find.lower() in data["data"]["description"].lower():
                            to_skip = True 
                    
        if to_skip:
            return {}

        to_parse = data["entities"]
        with_valid_code = []
        for x in to_parse:
            if x.get("geo", {}).get("feature_code") in self.valid_codes:
                with_valid_code.append(x["geo"])

        referenced_countries = [x["country_code3"] for x in with_valid_code]
        if not len(referenced_countries):
            return 
        
        referenced_regions = [x["admin1"] for x in with_valid_code]
        referenced_cities = [x["place_name"] for x in with_valid_code]
        
        locations = []

        for idx, country in enumerate(referenced_countries):
            location = {"country_code3": country}

            if referenced_regions and with_valid_code[idx]["feature_code"] != "PCLI":
                location["region"] = referenced_regions[idx]
            
            if referenced_cities and with_valid_code[idx]["feature_code"] != "PCLI":
                location["city"] = referenced_cities[idx]

            locations.append(location)

        return locations


    def run(self, raw_data):
        to_compare = []

        for result in raw_data:
            for user in result['data']:
                try:
                    description_language = detect(user["description"])
                except lang_detect_exception.LangDetectException:
                    continue            
                            
                description_language2 = cld3.get_language(user["description"])

                if description_language == description_language2.language and description_language == "en":
                    try:
                        result = self.mordecai_parser.geoparse(user["description"])
                        over_threshold = [x for x in result if x["country_conf"] > 0.6]

                        if over_threshold:
                            parsed = self.parse_mordecai({"data": user, "entities": result})
                            if parsed:
                                to_compare.append({"twitch_id": user["id"], "location": parsed, "description": user["description"]})
                    except Exception:
                        pass


        return to_compare
