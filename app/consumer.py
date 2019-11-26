#-*- coding: utf-8 -*-
from app.utils import cached_item
from config import *
import datetime
import json
from .models.product import Product
from ByHelpers.rabbit_engine import RabbitEngine
from ByHelpers import applogger
from .norm import map_product_keys as mpk
from psycopg2.extensions import TransactionRollbackError
import sys
import csv
from os import listdir
from os.path import isfile, join
import pandas as pd

messages = []
ack = []
nack = []

CONSUMER_BATCH_SZ = 100

# Logging
logger = applogger.get_logger()

# Rabbit instances
consumer = RabbitEngine({
    'queue':QUEUE_CATALOGUE,
    'routing_key': QUEUE_CATALOGUE,
    'socket_timeout': '2'},
    blocking=False)

#consumer.connect()

producer = RabbitEngine({
    'queue':QUEUE_ROUTING,
    'routing_key': QUEUE_ROUTING},
    blocking=True)


resender = RabbitEngine({
    'queue':QUEUE_CATALOGUE,
    'routing_key': QUEUE_CATALOGUE},
    blocking=True)

# Cache variable
cached_ps = {}

counter = 0

# Product Acumulator
pstack = []
last_updt = datetime.datetime.utcnow()


def process(new_item, reroute=True, commit=True):
    """ Method that processes all elements with defined rules
    """
    # Fetch Route key
    item_uuid = None
    route_key = new_item['route_key']
    logger.debug('Evaluating: {}'.format(route_key))
    # Reformat Prod Values
    _frmted = mpk.product(route_key, new_item)
    logger.debug('Formatted product!')
    p = Product(_frmted)
    # Verify if product in Cache
    if p.product_id is None:
        logger.warning("Incomming product has no Product ID: [{}]".format(p.source))
        return
    prod_uuid = Product.puuid_from_cache(cached_ps, p)
    if not prod_uuid:
        # logger.debug('Getting from db')
        # Verify if product exists
        prod_uuid = Product.get({
            'product_id': p.product_id,
            'source': p.source,
            }, _cols=['product_uuid', 'item_uuid'], limit=1)
        if prod_uuid and 'item_uuid' in prod_uuid[0]:
            cached_item.add_key(prod_uuid[0]['product_uuid'], prod_uuid[0]['item_uuid'])
    else:
        prod_uuid[0]['item_uuid'] = cached_item.get_item_uuid(prod_uuid[0]['product_uuid'])
        logger.debug("Got UUID from cache!")

    # if exists
    if prod_uuid:
        logger.debug('Found product ({} {})!'.format(p.source, prod_uuid if not isinstance(prod_uuid, list) else prod_uuid[0].get('product_uuid')))
        # Get product_uuid
        p.product_uuid = prod_uuid[0]['product_uuid']
        item_uuid = prod_uuid[0]['item_uuid']
        # If `item` update item
        if route_key == 'item':
            logger.debug('Found product, batch updating...') 
            if not p.save(pcommit=commit, _is_update=True, verified=True):
                raise Exception("Could not update product!")
            logger.info('Updated ({} {}) product!'.format(p.source, p.product_uuid))
    else:
        logger.debug('Could not find product, creating new one..')
        _needed_params = {'source','product_id', 'name'}
        if not _needed_params.issubset(p.__dict__.keys()):
            raise Exception("Required columns to create are missing in product. (source, product_id, name)")
        if not p.save(pcommit=commit, verified=True):
            raise Exception('Unable to create new Product ({} {})!'.format(p.source, p.product_uuid))
        logger.info('Created product ({} {})'.format(p.source, p.product_uuid))
    if route_key == 'price':
        # If price, update product_uuid and reroute
        new_item.update({'product_uuid': p.product_uuid, "item_id": item_uuid})
        if reroute:
            producer.send(new_item)
            logger.info("[price] Rerouted back ({})".format(new_item['product_uuid']))
    if not reroute:
        return new_item

#Rabbit MQ callback function
def callback(ch, method, properties, body):
    global counter
    global messages
    global ack
    global nack
    
    lim = 15
    new_item = json.loads(body.decode('utf-8'))
    logger.debug("New incoming product..")

    messages.append(new_item)
    tag = method.delivery_tag
    try:
        ch.basic_ack(delivery_tag = method.delivery_tag)
        ack.append(tag)
    except Exception as e:
        logger.error("Error with the Delivery tag, method. [Basic Acknowledgment]")
        logger.error(e)
        nack.append(tag)

    if len(messages) >= CONSUMER_BATCH_SZ:
        messages = treat_batch(messages)
        logger.info('{} ACK messages\n{} NACK messages'.format(len(ack), len(nack)))
        logger.info('{} failed messages'.format(len(messages)))
        ack = []
        nack = []


def process_files():
    filepath = PATH + 'files/'
    dfs = [
        {
            'name':filepath + f,
            'dataframe':pd.read_csv(filepath + f).fillna('')
        }
        for f in listdir(filepath) if (isfile(join(filepath, f)) and APP_NAME.lower() in f)
    ]
    return dfs


def treat_batch(messages):
    commit = False
    failed = []
    dataframes = []
    
    fname = PATH + 'files/' + APP_NAME.lower() + str(datetime.datetime.utcnow()).replace(' ', '_').replace('.', '_').replace(':', '_') + '.csv'

    pd.DataFrame(messages).to_csv(fname, index=False, quoting=csv.QUOTE_ALL)

    dfs = process_files()

    result = pd.concat([
        element['dataframe'] for element in dfs
    ])

    mx_len = result['name'].count()

    for i, row in result.iterrows():
        if i == (mx_len-1):
            commit = True

        # print(messages[i].get('description'))
        # print('Commit this?', commit)
        try:
            process(row.to_dict(), commit=commit)
        except Exception as ex:
            logger.error('Some process failed')
            logger.error(ex)
            failed.append(row.to_dict())
    
    [os.remove(element['name']) for element in dfs]

    return failed


def start():
    logger.info("Warming up caching IDS...")
    global cached_ps
    cached_ps = Product.create_cache_ids()
    logger.info("Done warmup, loaded {} values from {} sources"\
        .format(sum([len(_c) for _c in cached_ps.values()]), len(cached_ps)))
    logger.info("Starting listener at " + datetime.datetime.now().strftime("%y %m %d - %H:%m "))
    consumer.set_callback(callback)
    cached_item.item_cache(cached_item.MAXSIZE, cached_item.TTL)
    try:
        consumer.run()
    except Exception as e:
        logger.error("Couldn't connect to Rabbit!!")
        logger.error(e)