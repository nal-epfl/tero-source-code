import re
from game_processors.processor import Processor


class HuntShowdown(Processor):
    def __init__(self, img_type="bw"):
        super().__init__(img_type)
        self.normal_limits = {
            "bw": [-1, 900],
            "tiny": [-1, 90]
        }.get(img_type, [-1, 900])

        self.results_names = ["latency"]
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
        contains_ping = [x for x in value if "lat" in x["original"].lower() or "ten" in x["original"].lower() or "ncy" in x["original"].lower()]
        if contains_ping:
            return contains_ping[-1]["clean"]                   
        

    def get_values(self, values_to_parse):
        if not values_to_parse:
            return {}

        to_return = {}
        
        grouped = self.group_horizontally(values_to_parse, n_columns=2)

        for group in grouped:  
            whole_text = "".join([g[0] for g in group]).lower()
            
            clean_value = self.get_clean_values(whole_text)
            latency = self.analyze_single_value(clean_value)
            if latency:
                to_return["latency"] = latency
                to_return["has_mark"] = "lat" in whole_text or "ten" in whole_text or "ncy" in whole_text

        return to_return
    