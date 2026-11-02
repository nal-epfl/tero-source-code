FROM easyocr-updated

ENV TZ=UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime

COPY run_easyocr.py /app/run_easyocr.py
COPY config.py /app/config.py

WORKDIR /app

ENTRYPOINT ["python3", "run_easyocr.py"]
