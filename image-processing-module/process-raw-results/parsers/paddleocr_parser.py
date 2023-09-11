from parsers.parser import Parser


class PaddleOCRParser(Parser):
    def __init__(self, img_type="bw"):
        super().__init__(img_type)
        self.name = "paddleocr"
    
    
    def parse_single_match(self, match):
        return match[1][0], {"x1": min([x[0] for x in match[0]]), "x2": max([x[0] for x in match[0]]), "y1": min([x[1] for x in match[0]]), "y2": max([x[1] for x in match[0]])}


    def parse_line(self, line):
        return [self.parse_single_match(l) for l in line["results"]]