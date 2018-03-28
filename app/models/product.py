import datetime
from flask import g
from app import errors, logger
from config import *
import requests
from pprint import pformat as pf
import ast
import json
from app.norm.normalize_text import key_format

geo_stores_url = 'http://'+SRV_GEOLOCATION+'/store/retailer?key=%s'

class Product(object):
    """
        Class perform Query methods on PostgreSQL items
    """

    @staticmethod
    def get_one():
        """
            Static Method to verify correct connection with Items Postgres DB
        """
        try:
            q = g._db.query("SELECT * FROM product LIMIT 1").fetch()
        except:
            logger.error("Postgres Items Connection error")
            return False
        for i in q:
            logger.info('Product UUID: ' + str(i['product_uuid']))
        return {'msg':'Postgres Items One Working!'}