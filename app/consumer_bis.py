#-*- coding: utf-8 -*-
import datetime
import json
import sys
from os import listdir
import time
from os.path import isfile, join
from pprint import pprint

from ByHelpers import applogger
from ByHelpers.rabbit_engine import RabbitEngine

import pandas as pd
from app.utils import cached_item
from config import *

from .models.product import Product
from .norm import map_product_keys as mpk

# import psutil

# PROCNAME = "flask"

# for proc in psutil.process_iter():
#     if PROCNAME in proc.name():
#         print(proc)

# sys.exit()



CONSUMER_BATCH_SZ = 100

# Logging
logger = applogger.get_logger()

# Rabbit instances
consumer = RabbitEngine({
    'prefetch' : 300,
    'queue':QUEUE_CATALOGUE,
    'routing_key': QUEUE_CATALOGUE,
    'socket_timeout': '40',
    'heartbeat_interval': '0'},
    blocking=False)


producer = RabbitEngine({
    'queue':QUEUE_ROUTING,
    'routing_key': QUEUE_ROUTING},
    blocking=True)

# Cache variable
cached_ps = {}

counter = 0

# Product Acumulator
pstack = []
last_updt = datetime.datetime.utcnow()

to_update = []
to_insert = []
insert_dic = {}
updt_count = 0
insrt_count = 0
can_ack = False


def process(new_item, reroute=True, commit=True):
    """ Method that processes all elements with defined rules
    """
    global to_update
    global to_insert
    global updt_count
    global insert_dic
    global insrt_count
    global cached_ps
    
    can_ack = False
    item_uuid = None
    # Fetch Route key
    route_key = new_item['route_key']
    #logger.debug('Evaluating: {}'.format(route_key))
    # Reformat Prod Values
    _frmted = mpk.product(route_key, new_item)
    #logger.debug('Formatted product!')
    p = Product(_frmted)
    #logger.debug('Created product object!')

    # Verify if product in Cache
    if p.product_id is None:
        logger.warning("Incomming product has no Product ID: [{}]".format(p.source))
        return

    #logger.debug('Getting product_uuid from cache!')    
    prod_uuid = Product.puuid_from_cache(cached_ps, p.__dict__)

    if not prod_uuid:
        prod_uuid = Product.get({
            'product_id': p.product_id,
            'source': p.source,
            }, _cols=['product_uuid', 'item_uuid'], limit=1)

        if prod_uuid and prod_uuid[0].get('item_uuid'):
            cached_item.add_key(prod_uuid[0]['product_uuid'], prod_uuid[0]['item_uuid'])
            item_uuid = prod_uuid[0]['item_uuid']
    else:
        item_uuid = cached_item.get_item_uuid(prod_uuid[0]['product_uuid'])
        #logger.debug("Got UUID from cache!")
    
    # If product actually exists
    if prod_uuid:
        #logger.debug('Found product ({} {})!'.format(p.source, prod_uuid))
        prod_uuid = prod_uuid[0]['product_uuid']   
        p.product_uuid = prod_uuid

        # If `item` update item
        if route_key == 'item':
            to_update.append(p.__dict__)
            updt_count += 1
            logger.debug('To Update: {}'.format(updt_count))
    
    # If product is not in DB 
    else:
        logger.debug('Could not find product, creating new one..')
        _needed_params = {'source','product_id', 'name'}
        if not _needed_params.issubset(p.__dict__.keys()):
            raise Exception("Required columns to create are missing in product. (source, product_id, name)")

        if route_key == 'item':
            # Make sure this product was not already included in the insert list
            if _p.source in insert_dic.keys():
                    if _p.product_id not in insert_dic[_p.source]:
                        insert_dic[_p.source].add(_p.product_id)
                        to_insert.append(p.__dict__)
                        insrt_count += 1
                    else:
                        logger.info('Element already to be inserted')
                else:
                    insert_dic[_p.source] = {}

        if route_key == 'price':
            cols = ['product_id', 'gtin', 'item_uuid', 'source', 'name', 'description', 'images', 'categories', 'url', 'brand', 'provider', 'ingredients', 'raw_html', 'raw_product', 'last_modified']
            p_uuid_ls = Product.insert_batch_qry([p.__dict__], 'product', 'product_uuid', cols=cols)
            p.product_uuid = p_uuid_ls[0]
            update_cache(p)

    if updt_count >= CONSUMER_BATCH_SZ:
        try:
            logger.info('-------------- Batch updating ---------------------') 
            cols = ['product_uuid', 'product_id', 'gtin', 'item_uuid', 'source', 'name', 'description', 'images', 'categories', 'url', 'brand', 'provider', 'ingredients', 'raw_html', 'raw_product', 'last_modified']
            Product.update_prod_query(to_update, 'product', 'product_uuid', cols=cols)  
            updt_count = 0
            to_update = []
        except Exception as e:
            logger.error('Error updating batch')
            logger.error(e)
        
        can_ack = True
        
    if insrt_count >= CONSUMER_BATCH_SZ:
        try:
            logger.info('-------------- Batch inserting ---------------------') 
            cols = ['product_id', 'gtin', 'item_uuid', 'source', 'name', 'description', 'images', 'categories', 'url', 'brand', 'provider', 'ingredients', 'raw_html', 'raw_product', 'last_modified']
            # print(to_insert)
            Product.insert_batch_qry(to_insert, 'product', 'product_uuid', cols=cols)
            insrt_count = 0
            to_insert = []
        
        except Exception as e:
            logger.error('Error inserting batch')
            logger.error(e)

        can_ack = True

    if route_key == 'price':
        # If price, update product_uuid and reroute
        new_item.update({'product_uuid': p.product_uuid, "item_id": item_uuid})
        if reroute:
            producer.send(new_item)
            logger.info("[price] Rerouted back ({})".format(new_item['product_uuid']))

    return can_ack
 

#Rabbit MQ callback function
def callback(ch, method, properties, body):
    global counter

    t_0 = datetime.datetime.utcnow()
    
    new_item = json.loads(body.decode('utf-8'))
    #logger.debug("New incoming product..")
    can_ack = process(new_item)

    try:
        if can_ack:
            print('----------- CAN ACK ------------')
            ch.basic_ack(delivery_tag=method.delivery_tag, multiple=True)
    except Exception as e:
        logger.error("Error with the Delivery tag, method. [Basic Acknowledgment]")
        logger.error(e)
    print("LASTED FOR:")
    print((datetime.datetime.utcnow()-t_0))


def update_cache(_p):
    global cached_ps
    if _p.source not in cached_ps.keys():
        cached_ps[_p.source] = {}
    cached_ps[_p.source][_p.product_id] = _p.product_uuid
    logger.debug('Updated cache')


def start():
    global cached_ps
    logger.info("Warming up caching IDS...")
    cached_ps = Product.create_cache_ids()
    logger.info("Done warmup, loaded {} values from {} sources: ({} MB)"\
        .format(sum([len(_c) for _c in cached_ps.values()]), len(cached_ps), (sys.getsizeof(cached_ps)* 1000000 / 10**6)))
    logger.info("Starting listener at " + datetime.datetime.now().strftime("%y %m %d - %H:%m "))
    consumer.set_callback(callback)
    cached_item.item_cache(cached_item.MAXSIZE, cached_item.TTL)
    consumer.run()