from game_processors.processor import Processor
from config import default_resolution


class GenshinImpactProcessor(Processor):
    def __init__(self, img_type="bw"):
        super().__init__(img_type)
        self.normal_limits = {
            "bw": [120, 620],
            "tiny": [5, 70]
        }.get(img_type, [120, 620])

        self.results_names = ["latency"]
        self.x_threshold = 60
        self.y_threshold = 40
    

    def get_values(self, values_to_parse, image_info):
        to_return = {}
        
        for to_parse in values_to_parse:                 
            value, box = to_parse

            clean_value = self.get_clean_values(value)
            
            # if self.normal_limits[0] * image_info.get("resolution", 1080) / default_resolution < box["x1"] < self.normal_limits[1] * image_info.get("resolution", 1080) / default_resolution:
            latency = self.analyze_single_value(clean_value)
            if latency:
                to_return["latency"] = latency

        return to_return
    