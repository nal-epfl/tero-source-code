## Image processing module: process images

### Requirements
1. __EasyOCR__:
    * Create the base Docker image by building ``easy-ocr-base``.
    * Create the EasyOCR Docker image by:
        1. Replacing the configuration parameters found in ``run-easyocr/config.py``.
        2. Build the Docker image.
2. __PaddleOCR__:
    * Create the PaddleOCR Docker image by:
        1. Replacing the configuration parameters found in ``run-paddleocr/config.py``.
        2. Build the Docker image.


### Scripts
1. __run_processing__: Schedule image batches to be processed. Once all engines are done, compile and upload results and clean processed images.
2. __match_pytesseract__: Process images using Pytesseract.
3. __run-easyocr/run_easyocr.sh__: Start the EasyOCR Docker image. Parameters: local_volume GPU_index.
4. __run-paddleocr/run_paddleocr.sh__: Start the PaddleOCR Docker image. Parameters: local_volume GPU_index.



### Configuration parameters and secrets:
1. __batch_size__: Number of image batches to process at a time.
2. __processing_queues__: Redis queues for each OCR engine.
3. __img_storage__ / __results_storage__ / __to_process_storage__: Path to all images to process / store results / processing metadata. 
4. __long_term_storage__: Path to store result backups.
5. __max_simultaneous__: Max number of parallel processes.
6. __Tesseract-specific:__
    * __number_cores_tesseract__: Number of parallel Pytesseract processes.
    * __tesseract_config__: By default, all games are processed considering that the data is in a single line (PSM 7, see: https://tesseract-ocr.github.io/tessdoc/ImproveQuality.html), this configuration allows listing games that require a different type of PSM. 



