from parsers.parser import Parser

class PytesseractParser(Parser):
    def __init__(self, img_type="bw"):
        super().__init__(img_type)
        self.name = "tesseract"

    def parse_line(self, line):
        matches = []
        
        try:
            for idx in range(0, len(line["results"]["text"])):    
                matches.append([
                    line["results"]["text"][idx],
                    {"x1": line["results"]["left"][idx], "x2": line["results"]["left"][idx] + line["results"]["width"][idx],
                    "y1": line["results"]["top"][idx], "y2": line["results"]["top"][idx] + line["results"]["height"][idx]
                    }
                ])
        except Exception:
            pass
        
        return matches