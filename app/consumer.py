#-*- coding: utf-8 -*-
from config import *
import datetime
import json
from .models.product import Product
from .utils.rabbit_engine import RabbitEngine
from .utils import applogger
from .norm import map_product_keys as mpk
import sys

# Logging
logger = applogger.get_logger()

# Rabbit instances
consumer = RabbitEngine({
    'queue':QUEUE_CATALOGUE,
    'routing_key': QUEUE_CATALOGUE},
    blocking=False)

producer = RabbitEngine({
    'queue':QUEUE_ROUTING,
    'routing_key': QUEUE_ROUTING},
    blocking=True)

#Rabbit MQ callback function
def callback(ch, method, properties, body):
    new_item = json.loads(body.decode('utf-8'))
    logger.info("New incoming product..")
    logger.debug(new_item)
    try:
        # Fetch Route key
        route_key = new_item['route_key']
        del new_item['route_key']
        # Reformat Prod Values
        _frmted = mpk.product(route_key, new_item)
        # Verify if product exists
        # if exists
        ## get_prod_uuid
        ## if `item`
        ### update_prod
        # else
        ## save_new_prod
        # if `price`
        ## append_prod_uuid
        ## reroute_prod
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
        
            
