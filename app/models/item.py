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

class Item(object):
    """ Class perform Query methods on PostgreSQL items
    """

    __attrs__ = ['item_uuid', 'gtin', 'checksum', 'name',
        'description', 'last_modified']

    def __init__(self, params):
        """ Item constructor

            Params:
            -----
            params : dict
                Table fields to initialize model (gtin, name, etc.)
        """
        self.item_uuid = params['item_uuid'] \
            if 'item_uuid' in params else None
        self.name = params['name']
        self.description = params['description']
        self.gtin = str(params['gtin']).zfill(14)[-14:]

    def save(self):
        """ Class method to save Item record in DB 
        """
        logger.info("Saving Item...")
        if self.item_uuid:
            if not Item.exists({'item_uuid': self.item_uuid}):
                self.item_uuid = None
        elif Item.exists({'gtin': self.gtin}):
            self.message = 'Item already exists!'
            self.item_uuid = Item.get(['item_uuid'])['item_uuid']
            return True
        # Load model
        m_item = g._db.model('item', 'item_uuid')
        if self.item_uuid:
            m_item.item_uuid = self.item_uuid
        m_item.gtin = self.gtin
        m_item.checksum = int(self.gtin[-1])
        m_item.name = self.name
        m_item.description = self.description
        m_item.last_modified = str(datetime.datetime.utcnow())
        try:
            m_item.save()
            self.message = "Correctly stored Item!"
            logger.info(self.message)
        except Exception as e:
            logger.error(e)
            raise errors.ApiError(70002, "Issues saving in DB!")
    
    @staticmethod
    def exists(k_param):
        """ Static method to verify Item existance

            Params:
            -----
            k_param : dict
                Key-value element to query in Item table

            Returns:
            -----
            exists : bool
                Existance flag
        """
        logger.debug("Verifying Item existance...")
        _key, _val = list(k_param.items())[0]
        try:
            exists = g._db.query("""SELECT EXISTS (
                            SELECT 1 FROM item WHERE {} = '{}')"""\
                            .format(_key, _val))\
                        .fetch()[0]['exists']
        except Exception as e:
            logger.error(e)
            return False
        return exists

    @staticmethod
    def get_one():
        """ Static Method to verify correct connection 
            with Catalogue Postgres DB
        """
        try:
            q = g._db.query("SELECT * FROM item LIMIT 1").fetch()
        except:
            logger.error("Postgres Catalogue Connection error")
            return False
        for i in q:
            logger.info('Item UUID: ' + str(i['item_uuid']))
        return {'msg':'Postgres Catalogue One Working!'}


