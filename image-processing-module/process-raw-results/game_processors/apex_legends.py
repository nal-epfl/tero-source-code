import re
import json
from game_processors.processor import Processor


class ApexLegendsProcessor(Processor):
    def __init__(self, img_type="bw"):
        super().__init__(img_type)
        self.results_names = ["fps", "latency"]
        self.normal_limits = {
            "bw": [200],
            "tiny": [10]
        }.get(img_type, [200])


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
                        break
            
            clean_numbers = ""

            if numbers:
                clean_numbers = "".join(numbers)

            if clean_numbers:
                to_analyze.append({"contained_special": t != clean_numbers, "clean": clean_numbers, "original": t})
    
        return to_analyze

    def get_values(self, values_to_parse):
        if not values_to_parse:
            return {}

        to_return_candidates = {
            "fps": [],
            "latency": []
        }
        
        grouped = self.group_horizontally(values_to_parse)
        
        for group in grouped:                 
            whole_text = " ".join([g[0] for g in group]).lower()
            
            for val in group:
                value, box = val

                clean_value = self.get_clean_values(value)
                if clean_value:
                    if box["y1"] < self.normal_limits[0]:
                        fps = self.analyze_single_value(clean_value)
                        if fps:
                            to_return_candidates["fps"].append({"fps": fps, "original": whole_text, "has_mark": "fp" in whole_text or "ps" in whole_text})
                    else:
                        ping = self.analyze_single_value(clean_value)
                        if ping:
                            to_return_candidates["latency"].append({"latency": ping, "original": whole_text, "has_mark": "ng" in whole_text or "ms" in whole_text})

        to_return = {}

        if len(to_return_candidates["latency"]) == 1:
            to_return = to_return_candidates["latency"][0]
        else:
            latency_with_mark = [x for x in to_return_candidates["latency"] if x["has_mark"]]
            if len(latency_with_mark) == 1:
                to_return = latency_with_mark[0]
        
        return to_return
    