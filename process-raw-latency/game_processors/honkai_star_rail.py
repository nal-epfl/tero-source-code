from game_processors.processor import Processor
from config import default_resolution


class HonkaiStarRailProcessor(Processor):
    def __init__(self, img_type="bw"):
        super().__init__(img_type)
        self.normal_limits = {
            "bw": [150, 720],
            "tiny": [5, 70]
        }.get(img_type, [150, 720])

        self.results_names = ["latency"]
        self.x_threshold = 60
        self.y_threshold = 40
    

    def get_values(self, values_to_parse, image_info):
        to_return = {}
        
        for idx, to_parse in enumerate(values_to_parse):                 
            value, box = to_parse

            clean_value = self.get_clean_values(value)
                        
            # if self.normal_limits[0] * image_info.get("resolution", 1080) / default_resolution < box["x1"] < self.normal_limits[1] * image_info.get("resolution", 1080) / default_resolution:
            latency = self.analyze_single_value(clean_value)
            if latency and ("m" in value.lower() or "s" in value.lower()):
                to_return["latency"] = latency

            # A second alternative, the next value has the ms
            if latency and idx+1 < len(values_to_parse) and ("m" in values_to_parse[idx+1][0].lower() or "s" in values_to_parse[idx+1][0].lower()):
                to_return["latency"] = latency

        return to_return
    
    
    def process_areas(self, value):
        to_return = {}

        for variable in self.results_names:
            with_variable = []
            for area in value.values():
                if variable in area:
                    with_variable.append(area)

            if with_variable:
                if len(with_variable) == 1:
                    to_return = with_variable[0]
        
        return to_return
    
