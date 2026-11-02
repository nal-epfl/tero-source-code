import re
from game_processors.processor import Processor
from config import default_resolution


class MarvelRivalsProcessor(Processor):
    def __init__(self, img_type="bw"):
        super().__init__(img_type)
        self.limit_x = {
            "bw": 200,
            "tiny": 40
        }.get(img_type, 200)

        self.limit_y = {
            "bw": 350,
            "tiny": 45        
        }.get(img_type, 350)

        self.results_names = ["fps", "latency"]
        self.x_threshold = 60
        self.y_threshold = 40
    
    
    def get_clean_values(self, string):
        tokens = []
        
        space_tokens = re.split(r"\s", string)

        for t in space_tokens:
            tokens.extend(t.split("."))

        to_analyze = []

        for t in tokens:
            numbers = []
            
            for s in t:
                try:
                    int(s)
                    numbers.append(s)
                except ValueError:
                    if numbers:           
                        clean_numbers = "".join(numbers)

                        if clean_numbers:
                            to_analyze.append({"contained_special": t != clean_numbers, "clean": clean_numbers, "original": string})
                        
                        numbers = []

            if numbers:           
                clean_numbers = "".join(numbers)

                if clean_numbers:
                    to_analyze.append({"contained_special": t != clean_numbers, "clean": clean_numbers, "original": string})
            
        return to_analyze


    def analyze_single_value(self, value):
        contains_ping = [x for x in value if "pi" in x["original"].lower() or "ng" in x["original"].lower()]
        if contains_ping:
            if len(contains_ping) > 1:
                if len(contains_ping) == 2:
                    if contains_ping[0]["contained_special"] and not contains_ping[1]["contained_special"]:
                        return contains_ping[1]["clean"]
                    elif not contains_ping[0]["contained_special"] and contains_ping[1]["contained_special"]:
                        return contains_ping[0]["clean"]
            elif len(contains_ping) == 1:
                return contains_ping[0]["clean"]


    def get_values(self, values_to_parse, image_info):
        grouped = self.group_horizontally(values_to_parse)
        
        to_return = {}
        
        for group in grouped:
            # Special case: remove 
            whole_text = "".join([g[0] for g in group if ("lo" not in g[0].lower() or "st" not in g[0].lower())]).lower()

            clean_value = self.get_clean_values(whole_text)
            latency = self.analyze_single_value(clean_value)

            if latency:
                to_return["latency"] = latency

        return to_return