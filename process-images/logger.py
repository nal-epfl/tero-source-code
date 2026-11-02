import logging
import os
from logging.handlers import RotatingFileHandler

from config import base_path

def get_logger(log_file, app_name='root'):
    if not os.path.isdir("{}/logs".format(base_path)):
        os.makedirs("{}/logs".format(base_path))
    
    log_formatter = logging.Formatter('%(asctime)s %(levelname)s %(funcName)s(%(lineno)d) %(message)s',
                                      datefmt='%Y/%m/%d %I:%M:%S %z')

    my_handler = RotatingFileHandler('{}/logs/{}.log'.format(base_path, log_file), mode='a', maxBytes=5*1024*1024,
                                     backupCount=2, encoding=None, delay=0)
    my_handler.setFormatter(log_formatter)
    my_handler.setLevel(logging.INFO)

    app_log = logging.getLogger(app_name)
    app_log.setLevel(logging.INFO)

    app_log.addHandler(my_handler)

    return app_log
