from parsers.parser import Parser


class EasyOCRParser(Parser):
    def __init__(self, img_type="bw"):
        super().__init__(img_type)
        self.name = "easyocr"


    def parse_single_match(self, match):
        return match[1], {"x1": match[0][0][0], "x2": match[0][1][0], "y1": match[0][0][1], "y2": match[0][2][1]}


    def parse_line(self, line):
        return [self.parse_single_match(l) for l in line["results"]]