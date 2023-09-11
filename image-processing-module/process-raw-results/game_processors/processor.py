import re


class Processor:
    def __init__(self, img_type="bw"):
        self.results_names = []
        self.images_processed = 0
        self.img_type = img_type


    def group_horizontally(self, values, n_columns=1):
        values_to_parse = [x for x in values if x[0]]
        if not values_to_parse:
            return []
                
        groups = []
        
        for i in range(0, n_columns):
            if len(values_to_parse) > i:
                groups.append([values_to_parse[i]])

        if len(values_to_parse) == 1:
            return groups

        for to_parse in values_to_parse[n_columns:]:
            _, box = to_parse
            
            found_group = False
            for idx, group in enumerate(groups):
                min_group = min([g[1]["y1"] for g in group])
                max_group = max([g[1]["y2"] for g in group])

                if (box["y1"] <= min_group and box["y2"] >= min_group) or (box["y1"] <= max_group and box["y2"] >= max_group) or (box["y1"] >= min_group and box["y2"] <= max_group):
                    groups[idx].append(to_parse)
                    found_group = True
                    break        
            
            if not found_group:
                groups.append([to_parse])
        
        return [sorted(g, key=lambda x: x[1]["x1"]) for g in groups]


    def get_values(self, values):
        pass


    def get_clean_values(self, string):
        tokens = re.split(r"\s", string)

        to_analyze = []

        for t in tokens:
            if "m5" in t:
                t = t.replace("m5", "")
            
            if "m8" in t:
                t = t.replace("m8", "")

            clean_numbers = re.sub("[^0-9]", "", t)

            if clean_numbers:
                to_analyze.append({"contained_special": t != clean_numbers, "clean": clean_numbers, "original": t})
        
        return to_analyze


    def analyze_single_value(self, value):
        if len(value) > 1:
            if len(value) == 2:
                if value[0]["contained_special"] and not value[1]["contained_special"]:
                    return value[1]["clean"]
                elif not value[0]["contained_special"] and value[1]["contained_special"]:
                    return value[0]["clean"]
        elif len(value) == 1:
            return value[0]["clean"]


    def process_areas(self, value):
        if "0" in value:
            return value["0"]
        
        if "1" in value:
            return value["1"]
        
        return {}
