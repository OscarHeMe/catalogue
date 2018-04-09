import datetime
from flask import g
from app.utils import errors, applogger
from config import *
import requests
from pprint import pformat as pf
import ast
import json
from app.norm.normalize_text import key_format

geo_stores_url = 'http://'+SRV_GEOLOCATION+'/store/retailer?key=%s'
logger = applogger.get_logger()

class Product(object):
    """
        Class perform Query methods on PostgreSQL items
    """
    product_uuid = None
    item_uuid = None # REFERENCES "item" (item_uuid),
    source = None
    product_id = None
    name = None
    gtin = None
    description = None #text,
    normalized = None #text,
    raw_product = None #json,
    raw_html = None #text,
    categories = None #text,
    ingredients = None #text,
    brand = None #text,
    provider = None #text,
    url = None #text,
    images = None #text,
    last_modified = None #timestamp
    attributes = None 

    def __init__(self, *initial_data,**kwargs):
        # In case of dictionary initialization
        for dictionary in initial_data:
            for key in dictionary:
                if key in dir(self):
                    setattr(self, key, dictionary[key])
        # In case of keyworded initialization
        for key in kwargs:
            if key in self.__dict__.keys():
                setattr(self, key, kwargs[key])
        # Date conversion
        try:
            self.time = datetime.datetime.strptime(self.date, '%Y-%m-%d %H:%M:%S.%f')
        except:
            self.time = datetime.datetime.strptime(self.date, '%Y-%m-%d %H:%M:%S')
    
    @staticmethod
    def validate(elem):
        ''' Quick fields validation
        '''
        req_vars = ["source", "product_id", "name"]
        keys = list(elem.keys())
        # Si no tiene todas las keys requeridas regresamos False
        if not set(req_vars).issubset(keys):
            return False
        return True

    @property
    def as_dict(self):
        ''' Dictionary representation for saving to cassandra '''
        return {
            'product_id' : str(self.product_id),
            'name' : str(self.name),
            'gtin' : str(self.gtin) if self.gtin else None,
            'description' : str(self.description) if self.description else None,
            'raw_product' : str(self.raw_product) if self.raw_product else None,
            'raw_html' : str(self.raw_item) if self.raw_product else None,
            'categories' : ', '.join(self.categories), #string
            'source' : str(self.source),
            'last_modified' : self.time,
            'images' : ', '.join(self.images), 
            'url' : str(self.url) if self.url else None,
            'normalized' : str(self.normalized) if self.normalized else None, 
            'attributes' : self.attributes if isinstance(self.attributes, list) else [],
            'ingredients' : str(self.ingredients) if self.ingredients else None, 
            'brand' : str(self.brand) if self.brand else None, 
            'provider' : str(self.provider) if self.provider else None
        }

    #product_uuid = None
    #item_uuid = None # REFERENCES "item" (item_uuid),

    # --> normalized = None #text,
    
    def save_item(self):
        try:
            it_uuid = self.item_exists()
            item_model = g._db.model('item', 'item_uuid')
            if it_uuid is not None:
                item_model.item_uuid = it_uuid
            item_model.gtin = self.as_dict['gtin']
            item_model.name = self.as_dict['name']
            item_model.last_modified = self.as_dict['last_modified']
            item_model.save()
            logger.info("Saved Item!")
            self.item_uuid = item_model.last_id
            pr_uuid = self.get_product_uuid()
            if pr_uuid is not None:
                logger.debug('Product already exists')
            self.save_product(pr_uuid)
        except Exception as e:
            logger.error(e)
            logger.warning("Could not save Item")
            return False

    def item_exists():
        query = "SELECT item_uuid FROM item WHERE gtin = '{}' LIMIT 1"\
            .format(self.as_dict['gtin'])
        logger.debug(query)
        val = g._db.query(query).fetch()
        if len(val) > 0:
            val = val[0]['id_clss']
            logger.info('Item ID : {}'.format(val))
            # Store found product
            return val
        else:
            logger.info('New item found')
            return None


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

    def get_product_uuid(self):
        try:
            q = g._db.query("SELECT product_uuid FROM product WHERE source = '{}' and product_id = '{}' LIMIT 1 ".format(self.as_dic['source'], self.as_dict['product_id'])).fetch()
            if q:
                for el in q:
                    try:
                        return el['product_uuid']
                    except:
                        return None
        except:
            logger.error("Error while getting product from database")
            return None

    def save_product(self, pr_uuid):
        logger.info('Saving/Updating product')
        try:
            prod_model = g._db.model('product', 'product_uuid')
            # If Product UUID exists, it will update instead of save
            if pr_uuid is not None:
                prod_model.product_uuid = pr_uuid
            prod_model.item_uuid = self.item_uuid
            prod_model.product_id = self.as_dict['product_id']
            prod_model.name = self.as_dict['name']
            prod_model.gtin = self.as_dict['gtin']
            prod_model.description = self.as_dict['description']
            prod_model.normalized = self.as_dict['normalized']
            prod_model.source = self.as_dict['source']            
            prod_model.raw_product = self.as_dict['raw_product']
            prod_model.raw_html = self.as_dict['raw_html']
            prod_model.categories = self.as_dict['categories']
            prod_model.ingredients = self.as_dict['ingredients']
            prod_model.brand = self.as_dict['brand']
            prod_model.provider = self.as_dict['provider']
            prod_model.url = self.as_dict['url']
            prod_model.images = self.as_dict['images']
            prod_model.last_modified = self.as_dict['time']
            prod_model.save()
            logger.info("Saved Product!")
            # Save product uuid generated
            self.product_uuid = prod_model.last_id
            return True
        except Exception as e:
            logger.error(e)
            logger.warning("Could not save Product")
            return False
    
    def save_clsses(self):
        for _class in self.as_dict['attributes']:
            if isinstance(_class, dict):
                try:
                    #Verify if the class already exixts
                    class_id = self.class_exists(_class)
                    #Creates a class table model
                    clss_model = g._db.model('clss', 'id_clss')
                    #If class exists
                    if class_id:
                        logger.info('Class already exists')
                        #Assigns class id for updating
                        clss_model.id_clss = class_id
                    clss_model.name = _class['clss']
                    #clss_model.match 
                    clss_model.key = _class['clss']
                    #clss_model.description 
                    clss_model.source = self.as_dict['source']
                    #Saves class data
                    clss_model.save()
                    if not class_id:
                        #Gets class id for attribute matching
                        class_id = self.class_exists(_class)
                    #Checks if attribute already exists
                    attr_id = self.attr_exists(_class)
                    attr_model = g._db.model('attr', 'id_attr')
                    #If attr exists
                    if attr_id:
                        logger.info('Attr already exists')
                        #Assigns attr id for updating
                        attr_model.id_attr = attr_id
                    attr_model.name = _class['attr']
                    attr_model.key = _class['attr']
                    attr_model.id_clss = class_id
                    attr_model.source = self.as_dict['source']
                    #attr_model.has_value = 1 if _class['value'] else 0
                    #Saves attr data
                    attr_model.save()
                    #Already have id_attr
                    if not attr_id:
                        attr_id = self.attr_exists(_class)
                    prod_attr_id = self.attr_pr_exists(_class, attr_id)
                    attr_pr_model = g._db.model('attr_product', 'id_attr_product')
                    if prod_attr_id:
                        logger.info('Product attribute already exists')
                        attr_pr_model.id_attr_product = prod_attr_id
                    attr_pr_model.id_attr = attr_id
                    attr_pr_model.product_uuid = self.product_uuid
                    #attr_pr_model.source = self.source #Commented source table empty, must refer to it 
                    attr_pr_model.value = _class['value']
                    attr_pr_model.last_modified = self.as_dict['time']
                    attr_pr_model.save()
                    logger.info('Class and attr saved/updated')
                except Exception as e:
                    logger.warning(e)
                    logger.warning("Could not save Class")
                    return False
            else:
                logger.info('self.attributes must be a list of dictionaries, not {}'.format(type(_class)))
                return False
        return True
                

    def class_exists(self, class_dict):
        query = "SELECT id_clss FROM clss WHERE name = '{}' AND source = '{}' LIMIT 1"\
            .format(class_dict['clss'], self.as_dict['source'])
        logger.debug(query)
        val = g._db.query(query).fetch()
        if len(val) > 0:
            val = val[0]['id_clss']
            logger.info('Class ID : {}'.format(val))
            # Store found product
            return val
        else:
            logger.info('New class found')
            return None

    def attr_exists(self, class_dict):
        query = "SELECT id_attr FROM attr WHERE name = '{}' AND source = '{}' LIMIT 1"\
            .format(class_dict['attr'], self.as_dict['source'])
        logger.debug(query)
        val = g._db.query(query).fetch()
        if len(val) > 0:
            val = val[0]['id_attr']
            logger.info('Attr ID : {}'.format(val))
            # Store found product
            return val
        else:
            logger.info('New attr found')
            return None

    def attr_pr_exists(self, attr_dict, attr_id):
        query = "SELECT id_attr_product FROM attr_product WHERE id_attr = '{}' AND product_uuid = '{}' LIMIT 1"\
            .format(attr_id, self.product_uuid)
        logger.debug(query)
        val = g._db.query(query).fetch()
        if len(val) > 0:
            val = val[0]['id_attr_product']
            logger.info('Product attribute ID : {}'.format(val))
            # Store found product
            return val
        else:
            logger.info('New product attribute found')
            return None