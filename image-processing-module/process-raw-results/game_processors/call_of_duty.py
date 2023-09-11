from game_processors.processor import Processor


class CallOfDutyProcessor(Processor):
    def __init__(self, img_type="bw"):
        super().__init__(img_type)
        self.results_names = ["latency"]
        self.x_threshold = 60
        self.y_threshold = 40
    

    def get_values(self, values_to_parse):
        to_return_candidates = []
        
        contains_latency_mark = False
        whole_text = "".join(to_parse[0] for to_parse in values_to_parse)
        
        try:
            mark_location = [idx for idx, to_parse in enumerate(values_to_parse) if "ms" in to_parse[0].lower() or "mc" in to_parse[0].lower()][0]
        except Exception:
            mark_location = -1

        for idx, to_parse in enumerate(values_to_parse):                 
            value, box = to_parse

            # Important, CoD values in general come in two flavors: (\d+ Lat) and (: \d+ms). This check is to prevent the first case (which are values *before* the actual latency) to be
            # incorrectly assigned as latency values
            if "lat" in value.lower():
                contains_latency_mark = True
                break

            clean_value = self.get_clean_values(value)
            latency = self.analyze_single_value(clean_value)
            if latency:
                to_return_candidates.append({"latency": latency, "original": whole_text, "has_mark": mark_location >= 0, "location": idx})

        if not contains_latency_mark and to_return_candidates:
            return self.decide_candidates(mark_location, to_return_candidates)
        
        return {}

    def decide_candidates(self, mark_location, candidates):       
        # If the mark is not available, look for ":" or ";", fall back to appearance sorting otherwise
        prefered_location = -1
        if mark_location < 0:
            try:
                mark_location = candidates[0]["original"].lower().index(":" if ":" in candidates[0]["original"].lower() else ";")
                prefered_location = 1
            except Exception:
                mark_location = -1

        location_in_text = []

        for c in candidates:
            location_in_text.append([c["location"] - mark_location, c])
        
        # Depending on the marker (ms/mc vs :/;) the prefered location is before/after the marker
        return sorted(location_in_text, key=lambda x: (abs(x[0]), x[0]*prefered_location < 0))[0][1]


    def decide_tie(self, tied_values):
        for val in tied_values:
            if val["has_mark"]:
                return val

    
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
                else:
                    real_value = self.decide_tie(with_variable)
                    if real_value:
                        to_return = real_value
        
        return to_return
