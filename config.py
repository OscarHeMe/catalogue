# -*- coding: utf-8 -*-
import os
import sys

# General
APP_MODE = os.getenv('MODE', 'CONSUMER')
APP_NAME = 'catalogue-' + APP_MODE.lower()
APP_SECRET = os.getenv('APP_SECRET', '#catalogue')

# App directory
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
BASEDIR = BASE_DIR
PATH = os.path.dirname(os.path.realpath(__file__)) + "/"

# Env
TESTING = False
ENV = os.getenv('ENV', 'DEV')

# Logging and remote logging
LOG_LEVEL = os.getenv('LOG_LEVEL', 'WARNING')
LOG_HOST = os.getenv('LOG_HOST', 'logs5.papertrailapp.com')
LOG_PORT = os.getenv('LOG_PORT', 27971)

# DB vars
SQL_HOST = os.getenv('SQL_HOST', '127.0.0.1')
SQL_DB = os.getenv('SQL_DB', 'catalogue_us')
SQL_USER = os.getenv('SQL_USER', 'postgres')
SQL_PASSWORD = os.getenv('SQL_PASSWORD', '')
SQL_PORT = os.getenv('SQL_PORT', '5432')

# Services
SRV_GEOLOCATION = os.getenv('SRV_GEOLOCATION', 'gate.byprice.com/us/geolocation')

# Env dependent variables
SQL_DB = SQL_DB + "_test" if TESTING else SQL_DB
SQL_DB = SQL_DB + "_dev" if ENV.upper() == 'DEV' or ENV.upper() == 'DEVELOPMENT' else SQL_DB
SRV_GEOLOCATION = "dev." + SRV_GEOLOCATION if ENV.upper() == 'DEV' else SRV_GEOLOCATION

# Consumer vars
STREAMER = os.getenv('STREAMER', 'rabbitmq')
STREAMER_HOST = os.getenv('STREAMER_HOST', 'rabbitmq')
STREAMER_PORT = os.getenv('STREAMER_PORT', '5672')
STREAMER_EXCHANGE = os.getenv('STREAMER_EXCHANGE', 'data')
STREAMER_EXCHANGE_TYPE = os.getenv('STREAMER_EXCHANGE_TYPE', 'direct')

# Streamer
STREAMER_USER = os.getenv('STREAMER_USER', 'guest')
STREAMER_PASS = os.getenv('STREAMER_PASS', 'guest')
STREAMER_VIRTUAL_HOST = os.getenv('STREAMER_VIRTUAL_HOST', '')

QUEUE_ROUTING = "routing_dev" if ENV.upper() == 'DEV' else "routing"
QUEUE_CATALOGUE = 'catalogue_dev' if ENV.upper() == 'DEV' else 'catalogue'

# Stack and Time Limit to Update Batch
STACK_LIMIT = 20
TIME_LIMIT = 200
