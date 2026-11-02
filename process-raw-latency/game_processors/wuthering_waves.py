import re
from game_processors.processor import Processor
from config import default_resolution


class WutheringWavesProcessor(Processor):
    def __init__(self, img_type="bw"):
        super().__init__(img_type)
        self.normal_limits = {
            "bw": [400, 800],
            "tiny": [5, 70]
        }.get(img_type, [400, 800])

        self.results_names = ["latency"]
        self.x_threshold = 60
        self.y_threshold = 40
    

    def get_values(self, values_to_parse, image_info):
        to_return = {}
        
        candidates = []
        for to_parse in values_to_parse:                 
            value, box = to_parse
            # if "m"  not in value or "s" not in value:
            #     continue

            clean_value = self.get_clean_values(value)
            
            # if self.normal_limits[0] * image_info.get("resolution", 1080) / default_resolution < box["x1"] < self.normal_limits[1] * image_info.get("resolution", 1080) / default_resolution: 
            #     latency = self.analyze_single_value(clean_value)
            #     if latency:
            #         to_return["latency"] = latency
            # elif box["x1"] < self.normal_limits[0] * image_info.get("resolution", 1080) / default_resolution:
            latency = self.analyze_single_value(clean_value)
            if latency:
                if re.match(r'^(1)+', latency):
                    i = 0
                    for idx in range(0, len(latency)):
                        if latency[idx] != "1":
                            i = idx
                            break

                    if latency[i:]:
                        candidates.append([latency[:i], "m" in value.lower() or "s" in value.lower()])
          
                #         to_return["latency"] = latency[i:]
                else:
                   candidates.append([latency, "m" in value.lower() or "s" in value.lower()])             
        
        if len(candidates) == 1:
            if candidates[0][0]:
                to_return["latency"] = candidates[0][0]
        else:
            for x in candidates:
                if x[1] and x[0]:
                    to_return["latency"] = x[0]
                    
        return to_return
    
    