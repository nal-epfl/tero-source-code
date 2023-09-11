import re
from game_processors.processor import Processor


class Rainbow6(Processor):
    def __init__(self, img_type="bw"):
        super().__init__(img_type)
        self.normal_limits = {
            "bw": [700, 1400],
            "tiny": [-1, 90]
        }.get(img_type, [700, 1400])

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


    def get_values(self, values_to_parse):
        to_return_candidates = []
        original = [to_parse[0] for to_parse in values_to_parse]            
        
        for idx, to_parse in enumerate(values_to_parse):                 
            value, box = to_parse

            clean_value = self.get_clean_values(value)
            latency = self.analyze_single_value(clean_value)
            if latency:
                to_return_candidates.append({"latency": latency,  "location": idx})

        if to_return_candidates:
            return self.decide_candidates(original, to_return_candidates)
        
        return {}
    

    def decide_candidates(self, original, candidates):       
        for candidate in candidates:
            if "ping" in original[candidate["location"]].lower() or \
               (candidate["location"] > 0 and "ping" in original[candidate["location"]-1].lower()):
                return {**candidate, "original": "".join(original)}
        
        return {}
