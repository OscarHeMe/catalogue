import datetime
from flask import g
from app.utils import errors, applogger
from config import *
import requests
from pprint import pformat as pf
import ast
import json
from app.norm.normalize_text import key_format, tuplify

geo_stores_url = 'http://'+SRV_GEOLOCATION+'/store/retailer?key=%s'
logger = applogger.get_logger()

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
        self.name = params['name'] \
            if 'name' in params else None
        self.description = params['description'] \
            if 'description' in params else None
        self.gtin = str(params['gtin']).zfill(14)[-14:] \
            if 'gtin' in params else None

    def save(self):
        """ Class method to save Item record in DB 
        """
        logger.info("Saving Item...")
        # Verify for update
        if self.item_uuid:
            if not Item.exists({'item_uuid': self.item_uuid}):
                # If wants to update but wrong UUID, return Error                
                raise errors.ApiError(70006, "Cannot update, UUID not in DB!")
        # Verify for insert
        elif Item.exists({'gtin': self.gtin}):
            self.message = 'Item already exists!'
            self.item_uuid = Item.get(self.gtin)[0]['item_uuid']
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
            self.message = "Correctly {} Item!".format('updated' \
                if self.item_uuid else 'stored')
            m_item.save()
            self.item_uuid = m_item.last_id
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
    def get(_val, by='gtin', _cols=['item_uuid'], limit=None):
        """ Static method to get Item info

            Params:
            -----
            _val : str
                Value to query from
            by : str
                Column to query in
            _cols : list
                Columns to retrieve
            limit : int
                Elements to limit query

            Returns:
            -----
            _items : list
                List of elements
        """
        _cols = ','.join(_cols) if _cols else 'item_uuid'
        _query = "SELECT {} FROM item WHERE {} IN {}"\
            .format(_cols, by, tuplify(_val))
        if limit:
            _query += ' LIMIT {}'.format(limit)
        logger.debug(_query)
        try:
            _items = g._db.query(_query).fetch()
            logger.debug("Got {} items".format(len(_items)))
        except Exception as e:
            logger.error(e)
            raise errors.ApiError(70003, "Issues fetching elements in DB")
        return _items
    
    @staticmethod
    def delete(i_uuid):
        """ Static method to delete Item 

            Params:
            -----
            i_uuid : str
                Item UUID to delete

            Returns:
            -----
            resp : bool
                Transaction status
        """
        logger.debug("Deleting Item...")
        if not Item.exists({'item_uuid': i_uuid}):
            return {
                'message': "Item UUID not in DB!"
            }
        try:
            g._db.query("DELETE FROM item WHERE item_uuid='{}'"\
                        .format(i_uuid))
        except Exception as e:
            logger.error(e)
            raise errors.ApiError(70004, "Could not apply transaction in DB")
        return {
            'message': "Item ({}) correctly deleted!".format(i_uuid)
        }


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


    # @staticmethod
    # def get_items_index():
    #     """ Static Method to verify correct connection
    #         with Catalogue Postgres DB
    #     """
    #     try:
    #         q = g._db.query("SELECT * FROM item LIMIT 1").fetch()
    #     except:
    #         logger.error("Postgres Catalogue Connection error")
    #         return False
    #     for i in q:
    #         logger.info('Item UUID: ' + str(i['item_uuid']))
    #     return {'msg':'Postgres Catalogue One Working!'}

    @staticmethod
    def get_catalogue_uuids():
        """ Static Method to get the item_uuids and product_uuids from database
        """
        try:
            catalogue = g._db.query("""
                SELECT item_uuid as uuid, 'item_uuid' as type  
                    FROM item 
                UNION 
                SELECT product_uuid as uuid, 'product_uuid' as type 
                    FROM product 
                    WHERE item_uuid IS NULL
                """).fetch()
        except:
            logger.error("Postgres Catalogue Connection error")
            return False
        return catalogue
