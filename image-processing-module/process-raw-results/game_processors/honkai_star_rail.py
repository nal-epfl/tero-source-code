from game_processors.processor import Processor


class HonkaiStarRailProcessor(Processor):
    def __init__(self, img_type="bw"):
        super().__init__(img_type)
        self.normal_limits = {
            "bw": [-1, 720],
            "tiny": [-1, 70]
        }.get(img_type, [-1, 720])

        self.results_names = ["latency"]
        self.x_threshold = 60
        self.y_threshold = 40
    

    def get_values(self, values_to_parse):
        to_return = {}
        
        for to_parse in values_to_parse:                 
            value, box = to_parse

            clean_value = self.get_clean_values(value)
                        
            if self.normal_limits[0] < box["x1"] < self.normal_limits[1]:
                latency = self.analyze_single_value(clean_value)
                if latency:
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
    
