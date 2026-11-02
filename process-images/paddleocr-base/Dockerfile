FROM paddlepaddle/paddle:3.2.0-gpu-cuda12.6-cudnn9.5

RUN pip install paddlepaddle-gpu==3.2.0 paddleocr==3.3.3
RUN pip install opencv-python-headless tqdm

COPY download_model.py /home/download_model.py
WORKDIR /home

ENTRYPOINT ["python3", "download_model.py"]
