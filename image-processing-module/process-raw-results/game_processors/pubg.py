import re
from game_processors.processor import Processor


class PubgProcessor(Processor):
    def __init__(self, img_type="bw"):
        super().__init__(img_type)
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
                        break
            
            clean_numbers = ""

            if numbers:
                clean_numbers = "".join(numbers)

            if clean_numbers:
                to_analyze.append({"contained_special": t != clean_numbers, "clean": clean_numbers, "original": t})
        
        return to_analyze

    
    def analyze_single_value(self, value):
        if len(value) > 1:
            contains_m_or_s = [x for x in value if "m" in x["original"].lower() or "s" in x["original"].lower()]
            if len(contains_m_or_s) == 1:
                return contains_m_or_s[0]["clean"]               
        elif len(value) == 1:
            return value[0]["clean"]


    def get_values(self, values_to_parse):
        to_return = {}
        
        for to_parse in values_to_parse:                 
            value, _ = to_parse

            clean_value = self.get_clean_values(value)
            latency = self.analyze_single_value(clean_value)
            if latency:
                to_return["latency"] = latency
            
        return to_return
    