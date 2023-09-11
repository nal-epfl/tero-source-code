import re
from game_processors.processor import Processor


class TeamfightTacticsProcessor(Processor):
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
    

    def get_values(self, values_to_parse):
        to_return = {}
        
        for to_parse in values_to_parse:                 
            value, box = to_parse

            if re.match(r"\d\d[:,.]\d\d", value):
                return {}

            clean_value = self.get_clean_values(value)
            
            if box["y1"] < self.limit_y and box["x1"] > self.limit_x:
                latency = self.analyze_single_value(clean_value)
                if latency:
                    to_return["latency"] = latency
            elif box["y1"] > self.limit_y:
                fps = self.analyze_single_value(clean_value)
                if fps:
                    to_return["fps"] = fps

        return to_return