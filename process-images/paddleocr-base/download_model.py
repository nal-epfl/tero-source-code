from paddleocr import PaddleOCR

if __name__ == "__main__":
    ocr = PaddleOCR(
        use_doc_orientation_classify=False, # Disables document orientation classification model via this parameter
        use_doc_unwarping=False, # Disables text image rectification model via this parameter
        use_textline_orientation=False, # Disables text line orientation classification model via this parameter
        enable_mkldnn=False, 
        ocr_version="PP-OCRv4",
        device="gpu"
    )
