import json
import logging
import multiprocessing
import time
import os

import tornado.gen
import tornado.ioloop
import tornado.websocket
from tornado.options import options

import MQTTClient
import SerialProcess

log_level = os.getenv('RFLINK_LOG_LEVEL', 'INFO')
if logging.getLevelName(log_level) is not None:
    default_log_level = logging.getLevelName(log_level)
else:
    default_log_level = logging.INFO

file_log_level = default_log_level
stream_log_level = default_log_level

log_level = os.getenv('RFLINK_FILE_LOG_LEVEL', 'INFO')
if logging.getLevelName(log_level) is not None:
    file_log_level = logging.getLevelName(log_level)

log_level = os.getenv('RFLINK_STREAM_LOG_LEVEL', 'INFO')
if logging.getLevelName(log_level) is not None:
    stream_log_level = logging.getLevelName(log_level)

logger = logging.getLogger('RFLinkGW')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(funcName)s - %(levelname)s - %(message)s')
logger.setLevel(default_log_level)

fh = logging.FileHandler(os.getenv('RFLINK_LOG_FILE', '/var/log/RFLinkGateway.log'))
fh.setLevel(file_log_level)
fh.setFormatter(formatter)
logger.addHandler(fh)

#ch = logging.StreamHandler()
#ch.setFormatter(formatter)
#ch.setLevel(logging.DEBUG)
#logger.addHandler(ch)

def main():
    # messages read from device
    messageQ = multiprocessing.Queue()
    # messages written to device
    commandQ = multiprocessing.Queue()

    config_file = os.getenv('RFLINK_CONF_FILE', 'config.json')
    config = {}
    try:
        with open(config_file) as json_data:
            config = json.load(json_data)
    except Exception as e:
        logger.error("Config load failed")
        exit(1)

    sp = SerialProcess.SerialProcess(messageQ, commandQ, config)
    sp.daemon = True
    sp.start()

    mqtt = MQTTClient.MQTTClient(messageQ, commandQ, config)
    mqtt.daemon = True
    mqtt.start()

    # wait a second before sending first task
    time.sleep(1)
    options.parse_command_line()

    main_loop = tornado.ioloop.IOLoop.instance()
    main_loop.start()


if __name__ == "__main__":
    main()
