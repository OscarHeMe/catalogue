#!/usr/bin/env python
# -*- coding: utf-8 -*-

# File name: item
# Author: Oswaldo Cruz Simon
# Email: oswaldo_cs_94@hotmail.com
# Maintainer: Oswaldo Cruz Simon
# Date created: 15/10/19
# Date last modified: 15/10/19
# Project Name: catalogue

from cachetools import TTLCache
from app.models.product import Product
from ByHelpers import applogger

logger = applogger.get_logger()
# one hour 30 minutes
TTL = 5400
# max size of the dictionary if this size is reached the object deletes the oldest element
MAXSIZE = 10000

__item_cache = None


def item_cache(maxsize, ttl):
    global __item_cache
    if __item_cache:
        return item_cache
    else:
        __item_cache = TTLCache(maxsize=maxsize, ttl=ttl)
        return __item_cache


def get_item_uuid(product_uuid):
    """
    Get the information from cache memory or SQL database of a product using product_uuid
    Parameters
    ----------
    product_uuid : an product uuid

    Returns
    -------
    str:
        item_uuid

    """
    try:
        if product_uuid in __item_cache:
            return __item_cache[product_uuid]
        else:
            product_resultset = Product.get({
                'product_uuid': product_uuid
                }, _cols=['product_uuid', 'item_uuid'], limit=1)
            if product_resultset:
                __item_cache[product_uuid] = product_resultset[0]['item_uuid']
                return __item_cache[product_uuid]
            else:
                return None
    except Exception as e:
        logger.error("func=get_item_uuid, msg={}".format(e))
        raise e


def add_key(product_uuid, item_uuid):
    try:
        __item_cache[product_uuid] = item_uuid
    except Exception as e:
        logger.error("func=add_key, msg={}".format(e))
