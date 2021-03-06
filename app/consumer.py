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

messages = []
ack = []
nack = []
to_update = []
to_insert = []
insert_dic = {}
updt_count = 0
insrt_count = 0

# PROCNAME = "flask"

# for proc in psutil.process_iter():
#     if PROCNAME in proc.name():
#         print(proc)

# sys.exit()

CONSUMER_BATCH_SZ = 1

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

# last_received = datetime.datetime.utcnow()

to_update = []
to_insert = []
insert_dic = {}
updt_count = 0
insrt_count = 0
can_ack = False


def process(new_item, reroute=True, commit=False):
    """ Method that processes all elements with defined rules
    """
    global to_update
    global to_insert
    global insert_dic
    global updt_count
    global insert_dic
    global insrt_count
    global cached_ps
    # global last_received
    
    can_ack = False
    item_uuid = None

    if new_item:
        # Fetch Route key
        route_key = new_item['route_key']
        #logger.debug('Evaluating: {}'.format(route_key))
        # Reformat Prod Values
        _frmted = mpk.product(route_key, new_item)
        #logger.debug('Formatted product!')
        if _frmted['source'] not in cached_ps.keys():
            cached_ps.update(Product.create_cache_ids(_frmted['source']))

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

            if prod_uuid:
                p.prod_uuid = prod_uuid[0]['product_uuid']
                update_cache(p)
                if prod_uuid[0].get('item_uuid'):
                    cached_item.add_key(prod_uuid[0]['product_uuid'], prod_uuid[0]['item_uuid'])
                    item_uuid = prod_uuid[0]['item_uuid']            

        else:
            item_uuid = cached_item.get_item_uuid(prod_uuid[0]['product_uuid'])
            #logger.debug("Got UUID from cache!")
        
        # If product actually exists
        if prod_uuid:
            logger.debug('Found product')
            #logger.debug('Found product ({} {})!'.format(p.source, prod_uuid))
            prod_uuid = prod_uuid[0]['product_uuid']   
            p.product_uuid = prod_uuid
            p.item_uuid = item_uuid

            # If `item` update item
            if route_key == 'item':
                to_update.append(p.__dict__)
                updt_count += 1
                logger.debug('To Update: {}'.format(updt_count))
        
        # If product is not in DB 
        else:
            logger.debug('Could not find product, trying to create new one..')
            _needed_params = {'source','product_id', 'name'}
            if not _needed_params.issubset(p.__dict__.keys()):
                raise Exception("Required columns to create are missing in product. (source, product_id, name)")

            if route_key == 'item':
                # Make sure this product was not already included in the insert list
                if p.source not in insert_dic.keys():
                    insert_dic[p.source] = set()
                if p.product_id not in insert_dic[p.source]:
                    insert_dic[p.source].add(p.product_id)
                    to_insert.append(p.__dict__)
                    insrt_count += 1
                    logger.debug('To Insert: {}'.format(insrt_count))
                else:
                    logger.debug('Element already to be inserted')
                    
            if route_key == 'price':
                cols = ['product_id', 'gtin', 'item_uuid', 'source', 'name', 'description', 'images', 'categories', 'url', 'brand', 'provider', 'ingredients', 'raw_html', 'raw_product', 'last_modified']
                p_uuid_ls = Product.insert_batch_qry([p.__dict__], 'product', 'product_uuid', cols=cols)
                p.product_uuid = p_uuid_ls[0]
                update_cache(p)

        
        if route_key == 'price':
            # If price, update product_uuid and reroute
            new_item.update({'product_uuid': p.product_uuid, "item_id": item_uuid})
            can_ack = True
            if reroute:
                producer.send(new_item)
                logger.info("[price] Rerouted back ({})".format(new_item['product_uuid']))

    if updt_count >= CONSUMER_BATCH_SZ or commit:
        try:
            logger.info('-------------- Batch updating ---------------------') 
            cols = ['product_uuid', 'product_id', 'gtin', 'source', 'name', 'description', 'images', 'categories', 'url', 'brand', 'provider', 'ingredients', 'raw_html', 'raw_product', 'last_modified', 'is_active']
            Product.update_prod_query(to_update, 'product', 'product_uuid', cols=cols)  
            updt_count = 0
            to_update = []
        except Exception as e:
            logger.error('Error updating batch')
            logger.error(e)

        can_ack = True
        
    if insrt_count >= CONSUMER_BATCH_SZ or commit:
        try:
            logger.info('-------------- Batch inserting ---------------------') 
            cols = ['product_id', 'gtin', 'item_uuid', 'source', 'name', 'description', 'images', 'categories', 'url', 'brand', 'provider', 'ingredients', 'raw_html', 'raw_product', 'last_modified', 'is_active']
            # pprint(to_insert)
            Product.insert_batch_qry(to_insert, 'product', 'product_uuid', cols=cols)
            insrt_count = 0
            to_insert = []
        
        except Exception as e:
            logger.error('Error inserting batch')
            logger.error(e)

        can_ack = True        


    return can_ack
 


def update_cache(_p):
    global cached_ps
    if _p.source not in cached_ps.keys():
        cached_ps[_p.source] = {}
    cached_ps[_p.source][_p.product_id] = _p.product_uuid
    logger.debug('Updated cache')


#Rabbit MQ callback function
def callback(ch, method, properties, body):
    global counter
    # global last_received

    # last_received = datetime.datetime.utcnow()
    
    new_item = json.loads(body.decode('utf-8'))
    #logger.debug("New incoming product..")
    can_ack = process(new_item)

    try:
        if can_ack:
            logger.debug('Ack messages')
            ch.basic_ack(delivery_tag=method.delivery_tag, multiple=True)
    except Exception as e:
        logger.error("Error with the Delivery tag, method. [Basic Acknowledgment]")
        logger.error(e)
    # print("LASTED FOR:")
    # print((datetime.datetime.utcnow()-t_0))


def update_cache(_p):
    global cached_ps
    if _p.source not in cached_ps.keys():
        cached_ps[_p.source] = {}
    cached_ps[_p.source][_p.product_id] = _p.product_uuid
    logger.debug('Updated cache')


def start():
    logger.info("Starting listener at " + datetime.datetime.now().strftime("%y %m %d - %H:%m ") + 'from {}'.format(QUEUE_CATALOGUE))
    consumer.set_callback(callback)
    cached_item.item_cache(cached_item.MAXSIZE, cached_item.TTL)
    consumer.run()
