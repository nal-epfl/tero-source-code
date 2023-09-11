from game_processors.processor import Processor


class AmongUsProcessor(Processor):
    def __init__(self,img_type="bw"):
        super().__init__(img_type)

    def get_values(self, values_to_parse):
        if not values_to_parse:
            return {}

        to_return = {}
        
        grouped = self.group_horizontally(values_to_parse)

        for group in grouped:  
            whole_text = "".join([g[0] for g in group]).lower()
            
            clean_value = self.get_clean_values(whole_text)
            latency = self.analyze_single_value(clean_value)
            if latency:
                to_return["latency"] = latency
                to_return["has_mark"] = "ms" in whole_text

        return to_return