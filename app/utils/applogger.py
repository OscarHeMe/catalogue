#-*- coding: utf-8-*-
import socket
import logging
from logging.handlers import SysLogHandler
from config import *

logger = None

def create_logger(name=APP_NAME, level=LOG_LEVEL, host=LOG_HOST, port=LOG_PORT):
    ''' Create logger and add handlers and filters
    '''
    global logger
    log_format = '%(asctime)s %(service)s %(hostname)s [%(env)s] %(levelname)s: %(message)s'
    log_formatter = logging.Formatter(log_format,datefmt='%b %d %H:%M:%S')
    log_filter = ContextFilter()
    remote_handler = SysLogHandler(address=(LOG_HOST, int(LOG_PORT)))
    remote_handler.setFormatter(log_formatter)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)

    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging,LOG_LEVEL))
    logger.addFilter(log_filter)
    logger.addHandler(console_handler)
    logger.addHandler(remote_handler)


def get_logger():
    ''' Get the app logger by the app name
    '''
    return logging.getLogger(APP_NAME)


class ContextFilter(logging.Filter):
    hostname = socket.gethostname()
    service = APP_NAME
    env = ENV

    def filter(self, record):
        record.env = ContextFilter.env
        record.hostname = ContextFilter.hostname
        record.service = ContextFilter.service
        return True
