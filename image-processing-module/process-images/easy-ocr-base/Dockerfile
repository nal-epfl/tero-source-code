FROM anibali/pytorch

ENV TZ=UTC
RUN sudo ln -snf /usr/share/zoneinfo/$TZ /etc/localtime

# Install system libraries required by OpenCV.
RUN sudo apt-get update \
 && sudo apt-get install -y libgl1-mesa-glx libgtk2.0-0 libsm6 libxext6 \
 && sudo rm -rf /var/lib/apt/lists/*

RUN pip install easyocr redis
RUN pip install opencv-python-headless==4.5.4.60

COPY download-model.py /app/download-model.py
WORKDIR /app

CMD ["python3", "download-model.py"]
