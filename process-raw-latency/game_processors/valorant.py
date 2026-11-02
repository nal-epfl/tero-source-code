import re
from game_processors.processor import Processor


class Valorant(Processor):
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
            tokens.append(t)

        to_analyze = []

        for t in tokens:
            numbers = []
            
            for s in t:
                try:
                    # Invalid format: .xx, there has to be at least one digit before the .
                    if s == '.' and len(numbers) > 0:
                        numbers.append(s)
                    else:
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
        if len(value) == 1:
            return value[0]["clean"]    
        elif len(value) == 2:
            return [x["clean"] for x in value]
            

    def get_values(self, values_to_parse, image_info):
        to_return_candidates = []
          
        accumulated_label = ""

        for to_parse in values_to_parse:                 
            value, _ = to_parse

            clean_value = self.get_clean_values(value)
            accumulated_label += value

            if clean_value:
                latency = self.analyze_single_value(clean_value)
                if latency:
                    to_return_candidates.append({"latency": latency, "previous_label": accumulated_label})
                    accumulated_label = ""
                           
        if to_return_candidates:
            return self.decide_candidates(to_return_candidates)
        
        return {}
    
    
    def check_if_network(self, text):
        return "net" in text.lower() or "rtt" in text.lower() or "avg" in text.lower() or "max" in text.lower()


    def decide_candidates(self, candidates):       
        for candidate in candidates:
            if self.check_if_network(candidate["previous_label"]) and len(candidate["latency"]) > 0:
                return {"latency": candidate["latency"][0], "original": candidate["previous_label"]}
        
        return {}
