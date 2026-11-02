FROM registry.baidubce.com/paddlepaddle/paddle:2.1.3-gpu-cuda11.2-cudnn8 

RUN pip install paddlepaddle-gpu paddleocr redis
RUN pip install opencv-python-headless==4.5.4.60

COPY run_paddleocr.py /home/run_paddleocr.py
COPY config.py /home/config.py

COPY models_v2/en_ppocr_mobile_v2.0_det_infer.tar /root/.paddleocr/2.4/ocr/det/en/
RUN cd /root/.paddleocr/2.4/ocr/det/en/ && tar xf en_ppocr_mobile_v2.0_det_infer.tar

COPY models_v2/en_number_mobile_v2.0_rec_infer.tar /root/.paddleocr/2.4/ocr/rec/en/
RUN cd /root/.paddleocr/2.4/ocr/rec/en/ && tar xf en_number_mobile_v2.0_rec_infer.tar

COPY models_v2/ch_ppocr_mobile_v2.0_cls_infer.tar /root/.paddleocr/2.4/ocr/cls/
RUN cd /root/.paddleocr/2.4/ocr/cls && tar xf ch_ppocr_mobile_v2.0_cls_infer.tar

WORKDIR /home

ENTRYPOINT ["python3", "run_paddleocr.py"]
