#-*- coding: utf-8 -*-
from config import *
import datetime
import json
from .models import item
from .rabbit_engine import RabbitEngine
from . import applogger
import sys

# Logging
logger = applogger.get_logger()

# Rabbit instances
consumer = RabbitEngine({
    'queue':QUEUE_ROUTING,
    'routing_key': QUEUE_ROUTING},
    blocking=False)

#Rabbit MQ callback function
def callback(ch, method, properties, body):
    new_item = json.loads(body.decode('utf-8'))
    logger.debug("Debugging new item")
    logger.debug(new_item)
    try:
        ###
        # Hacer aqui todo el flow del consumer
        ###
    except Exception as e:
        logger.error(e)
        logger.warning("Could not save product in DB!")
    try: 
        ch.basic_ack(delivery_tag = method.delivery_tag)
    except Exception as e:
        logger.error("Error with the Delivery tag, method. [Basic Acknowledgment]")
        logger.error(e)

def start():
    logger.info("Started listener at " + datetime.datetime.now().strftime("%y %m %d - %H:%m "))
    consumer.set_callback(callback)
    try:
        consumer.run()
    except Exception as e:
        logger.error("Couldn't connect to Rabbit!!")
        logger.error(e)
        
            
