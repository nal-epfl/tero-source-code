from game_processors.processor import Processor


class Dota2Processor(Processor):
    def __init__(self, img_type="bw"):
        super().__init__(img_type)
               
        self.limits_x = {
            "bw": 500,
            "tiny": 30
        }.get(img_type, 500)
        
        self.limits_y = {
            "bw": 200,
            "tiny": 15
        }.get(img_type, 200)

        self.results_names = ["fps", "latency", "loss_in", "loss_out"]
        self.x_threshold = 60
        self.y_threshold = 40
    
    def get_values(self, values_to_parse):
        to_return = {}
        
        for to_parse in values_to_parse:                 
            value, box = to_parse

            clean_value = self.get_clean_values(value)
            
            if box["y1"] < self.limits_y:
                if box["x1"] < self.limits_x:
                    fps = self.analyze_single_value(clean_value)
                    if fps:
                        to_return["fps"] = fps
                elif self.limits_x < box["x1"]:
                    latency = self.analyze_single_value(clean_value)
                    if latency:
                        to_return["latency"] = latency
            else:
                if box["x1"] < self.limits_x:
                    loss_in = self.analyze_single_value(clean_value)
                    if loss_in:
                        to_return["loss_in"] = loss_in
                elif self.limits_x < box["x1"]:
                    loss_out = self.analyze_single_value(clean_value)
                    if loss_out:
                        to_return["loss_out"] = loss_out                

        return to_return
    