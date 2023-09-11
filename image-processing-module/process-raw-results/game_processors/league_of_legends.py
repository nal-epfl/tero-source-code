import re
from game_processors.processor import Processor


class LeagueOfLegendsProcessor(Processor):
    def __init__(self,img_type="bw"):
        super().__init__(img_type)
        self.normal_limits = {
            "bw": [800, 900],
            "tiny": [50,55]
        }.get(img_type, [800,900])

        self.results_names = ["fps", "latency"]
        self.x_threshold = 60
        self.y_threshold = 160
    

    def get_values(self, values_to_parse):
        to_return = {}
        
        for to_parse in values_to_parse:                 
            value, box = to_parse

            if re.match(r"\d\d[:,.]\d\d", value):
                return {}

            clean_value = self.get_clean_values(value)
            
            if box["x1"] < self.normal_limits[0]:
                fps = self.analyze_single_value(clean_value)
                if fps:
                    to_return["fps"] = fps
            elif box["x1"] > self.normal_limits[1]:
                ping = self.analyze_single_value(clean_value)
                if ping and "latency" not in to_return:
                    to_return["latency"] = ping

        return to_return
