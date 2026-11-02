from game_processors.processor import Processor
from config import default_resolution


class ArknightsEndfieldProcessor(Processor):
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
        
        for idx, to_parse in enumerate(values_to_parse):                 
            value, box = to_parse

            clean_value = self.get_clean_values(value)
            
            latency = self.analyze_single_value(clean_value)
            if latency and ("m" in value or "s" in value):
                to_return["latency"] = latency

            # A second alternative, the next value has the ms
            if latency and idx+1 < len(values_to_parse) and ("m" in values_to_parse[idx+1][0] or "s" in values_to_parse[idx+1][0]):
                to_return["latency"] = latency

        return to_return
    