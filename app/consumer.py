#-*- coding: utf-8 -*-
from config import *
import datetime
import json
from .models.product import Product
from ByHelpers.rabbit_engine import RabbitEngine
from ByHelpers import applogger
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
cached_ps = {}

# Product Acumulator
pstack = []
last_updt = datetime.datetime.utcnow()


def process(new_item, reroute=True):
    """ Method that processes all elements with defined rules
    """
    # Fetch Route key
    route_key = new_item['route_key']
    logger.debug('Evaluating: {}'.format(route_key))
    # Reformat Prod Values
    logger.debug("New item: {}".format(str(new_item.get('nutriments'))))
    _frmted = mpk.product(route_key, new_item)
    logger.debug('Formatted product!')
    logger.debug("formated item: {}".format(str(_frmted.get('nutriments'))))
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
        logger.debug("Got UUID from cache!")
    # if exists
    if prod_uuid:
        logger.debug('Found product!')
        # Get product_uuid
        p.product_uuid = prod_uuid[0]['product_uuid']
        # If `item` update item
        if route_key == 'item':
            logger.debug('Found product, batch updating...') 
            if not p.save(pcommit=True, _is_update=True, verified=True):
                raise Exception("Could not update product!")
            logger.info('Updated ({}) product!'.format(p.product_uuid))
    else:
        logger.debug('Could not find product, creating new one..')
        _needed_params = {'source','product_id', 'name'}
        if not _needed_params.issubset(p.__dict__.keys()):
            raise Exception("Required columns to create are missing in product. (source, product_id, name)")
        if not p.save(pcommit=True, verified=True):
            raise Exception('Unable to create new Product!')
        logger.info('Created product ({})'.format(p.product_uuid))
    if route_key == 'price':
        # If price, update product_uuid and reroute
        new_item.update({'product_uuid': p.product_uuid})
        if reroute:
            producer.send(new_item)
            logger.debug("[price] Rerouted back ({})".format(new_item['product_uuid']))
    if not reroute:
        return new_item

#Rabbit MQ callback function
def callback(ch, method, properties, body):
    try:
        new_item = json.loads(body.decode('utf-8'))
    except Exception as e:
        logger.error("Error reading json in callback: {}".format(str(e)))

    try:
        logger.debug("New incoming product..")
        process(new_item)
    except Exception as e:
        logger.error("Error processing item: {}".format(str(e)))

    try: 
        ch.basic_ack(delivery_tag = method.delivery_tag)
    except Exception as e:
        logger.error("Error with the Delivery tag, method. [Basic Acknowledgment]: {}".format(str(e)))

def start():
    logger.info("Warming up caching IDS...")
    global cached_ps
    cached_ps = Product.create_cache_ids()
    logger.info("Done warmup, loaded {} values from {} sources"\
        .format(sum([len(_c) for _c in cached_ps.values()]), len(cached_ps)))
    logger.info("Starting listener at " + datetime.datetime.now().strftime("%y %m %d - %H:%m "))
    consumer.set_callback(callback)
    try:
        consumer.run()
    except Exception as e:
        logger.error("Error in catalogue consumer: {}".format(str(e)))
