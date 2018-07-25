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

# Cache variable
cached_ps = None 


def process(new_item, reroute=True):
    """ Method that processes all elements with defined rules
    """
    # Fetch Route key
    route_key = new_item['route_key']
    logger.info('Evaluating: {}'.format(route_key))
    # Reformat Prod Values
    _frmted = mpk.product(route_key, new_item)
    logger.info('Formatted product!')
    p = Product(_frmted)
    # Verify if product in Cache
    prod_uuid = Product.puuid_from_cache(cached_ps, p)
    if not prod_uuid:
        # Verify if product exists
        prod_uuid = Product.get({
            'product_id': p.product_id,
            'source': p.source,
            }, limit=1)
    else:
        logger.info("Got UUID from cache!")
    # if exists
    if prod_uuid:
        logger.info('Found product!')
        # Get product_uuid
        p.product_uuid = prod_uuid[0]['product_uuid']
        # If `item` update item
        if route_key == 'item':
            logger.info('Found product, updating...')
            p.save()
            logger.info('Updated ({}) product!'.format(p.product_uuid))
    else:
        logger.info('Could not find product, creating new one..')
        _needed_params = {'source','product_id', 'name'}
        if not _needed_params.issubset(p.__dict__.keys()):
            raise Exception("Required columns to create are missing in product. (source, product_id, name)")
        if not p.save():
            raise Exception('Unable to create new Product!')
        logger.info('Created product ({})'.format(p.product_uuid))
    if route_key == 'price':
        # If price, update product_uuid and reroute
        new_item.update({'product_uuid': p.product_uuid})
        if reroute:
            producer.send('routing', new_item)
        logger.info("Rerouted back ({})".format(new_item['product_uuid']))
    if not reroute:
        return new_item

#Rabbit MQ callback function
def callback(ch, method, properties, body):
    new_item = json.loads(body.decode('utf-8'))
    logger.info("New incoming product..")
    logger.debug(new_item)
    try:
        process(new_item)
    except Exception as e:
        logger.error(e)
        logger.warning("Could not save product in DB!")
    try: 
        ch.basic_ack(delivery_tag = method.delivery_tag)
    except Exception as e:
        logger.error("Error with the Delivery tag, method. [Basic Acknowledgment]")
        logger.error(e)

def start():
    logger.info("Warming up caching IDS...")
    global cached_ps
    cached_ps = Product.create_cache_ids()
    logger.debug("Done warmup, loaded {} values from {} sources"\
        .format(sum([len(_c) for _c in cached_ps.values()]), len(cached_ps)))
    logger.info("Starting listener at " + datetime.datetime.now().strftime("%y %m %d - %H:%m "))
    consumer.set_callback(callback)
    try:
        consumer.run()
    except Exception as e:
        logger.error("Couldn't connect to Rabbit!!")
        logger.error(e)
        
            
