import datetime
from flask import g
from app import errors, logger
from config import *
import requests
from pprint import pformat as pf
import ast
import json
from app.models.category import Category
from app.norm.normalize_text import key_format, tuplify

geo_stores_url = 'http://'+SRV_GEOLOCATION+'/store/retailer?key=%s'

class Product(object):
    """ Class perform insert, update and query methods 
        on PSQL Catalogue.product 
    """

    __attrs__ = ['product_uuid', "product_id", "gtin", "item_uuid",
        "source", "name", "description", "images", "normalized",
        "categories", "url", "brand", "provider", "attributes",
        "ingredients","raw_html", "raw_product"]
    
    def __init__(self, _args):
        """ Product constructor

            Params:
            -----
            _args : dict
                All arguments to build a `Product` record
        """
        # Arguments verification and addition
        for _k in self.__attrs__:
            if _k in _args:
                self.__dict__[_k] = _args[_k]
                continue
            self.__dict__[_k] = None
        # Args Aggregation
        self.gtin = str(self.gtin).zfill(14)[-14:] if self.gtin else None
        self.product_id = str(self.product_id).zfill(20)[-255:] if self.product_id else None
        try:
            self.raw_product = json.dumps(_args)
        except Exception as e:
            logger.error(e)
            raise errors.ApiError(70005, "Wrong DataType to serialize for Product!")
        # Args validation
        try:
            assert isinstance(self.images, list) or isinstance(self.images, None)
            assert isinstance(self.categories, str) or isinstance(self.categories, None)
            assert isinstance(self.attributes, list) or isinstance(self.attributes, None)
            assert isinstance(self.raw_html, str) or isinstance(self.raw_html, None)
        except Exception as e:
            logger.error(e)
            raise errors.ApiError(70005, "Wrong DataType to save Product!")

    def save(self):
        """ Class method to save Product record in DB 
            with product_image, product_attr and product_category
        """
        logger.info("Saving Product...")
        # Verify for update
        if self.product_uuid:
            if not Product.exists({'product_uuid': self.product_uuid}):
                # If wants to update but wrong UUID, return Error                
                raise errors.ApiError(70006, "Cannot update, UUID not in DB!")
        # Verify for insert
        elif Product.exists({'product_id': self.product_id, 'source': self.source}):
            self.message = 'Product already exists!'
            self.product_uuid = Product.get({'product_id': self.product_id,
                                            'source': self.source})\
                                        [0]['product_uuid']
            return True
        # Load model
        m_prod = g._db.model('product', 'product_uuid')
        for _k in self.__attrs__:
            if _k != 'attributes' and self.__dict__[_k]:
                m_prod.__dict__[_k] = self.__dict__[_k]
        # Add date
        m_prod.last_modified = str(datetime.datetime.utcnow())
        try:
            self.message = "Correctly {} Product!".format('updated' \
                if self.product_uuid else 'stored')
            m_prod.save()
            self.product_uuid = m_prod.last_id
            logger.info(self.message)
            # Save product images
            if self.images:
                self.save_images()
            # Save product categories
            if self.categories:
                self.save_categories()
            # Save product attrs
            
        except Exception as e:
            logger.error(e)
            raise errors.ApiError(70002, "Issues saving in DB!")
        return True

    def save_images(self):
        """ Class method to save product images
        """
        for _img in self.images:
            try:
                _exist = g._db.query("""SELECT EXISTS (
                        SELECT 1 FROM product_image
                        WHERE product_uuid = '{}'
                        AND image = '{}')"""\
                        .format(self.product_uuid, _img))\
                    .fetch()[0]['exists']
                if _exist:
                    logger.info("Image already in DB!")
                    continue
                m_prod_im = g._db.model('product_image', 'id_product_image')
                m_prod_im.product_uuid = self.product_uuid
                m_prod_im.image = _img
                m_prod_im.last_modified = str(datetime.datetime.utcnow())
                m_prod_im.save()
                logger.info("Product Image correctly saved! ({})"\
                    .format(m_prod_im.last_id))
            except Exception as e:
                logger.error(e)
                logger.warning("Could not save Product image!")
        return True
    
    def save_categories(self):
        """ Class method to save product categories
        """
        _parent = None
        for _cat in self.categories.split(','):
            try:
                # Get ID if exists, otherwise create category                
                id_cat = Category.get_id(_cat, self.source)
                if not id_cat:
                    categ = Category({
                                'source': self.source,
                                'id_parent' : Category.get_id(_cat, self.source, 'id_parent'),
                                'name': _cat
                            })
                    id_cat = categ.save()
                    # Emergency skip
                    if not id_cat:
                        continue
                # Verify product category does not exist
                _exists = g._db.query("""SELECT EXISTS (
                        SELECT 1 FROM product_category
                        WHERE id_category = {}
                        AND product_uuid = '{}')"""\
                        .format(id_cat, self.product_uuid))\
                    .fetch()[0]['exists']
                if _exists:
                    logger.info("Category already assigned to Product!")
                    continue
                m_prod_cat = g._db.model('product_category', 'id_product_category')
                m_prod_cat.product_uuid = self.product_uuid
                m_prod_cat.id_category = id_cat
                m_prod_cat.last_modified = str(datetime.datetime.utcnow())
                m_prod_cat.save()
                logger.info("Product Category correctly saved! ({})"\
                    .format(m_prod_cat.last_id))
                # Save category as Product attribute
                # _attr = Attr()
                # _attr.save()
            except Exception as e:
                logger.error(e)
                logger.warning("Could not save Product category!")
        return True

    @staticmethod
    def exists(k_param):
        """ Static method to verify Product existance

            Params:
            -----
            k_param : dict
                Key-value element to query in Product table

            Returns:
            -----
            exists : bool
                Existance flag
        """
        logger.debug("Verifying Product existance...")
        _where = ' AND '.join(["{}='{}'".format(*z) \
                            for z in list(k_param.items())])
        try:
            exists = g._db.query("""SELECT EXISTS (
                            SELECT 1 FROM product WHERE {})"""\
                            .format(_where))\
                        .fetch()[0]['exists']
        except Exception as e:
            logger.error(e)
            return False
        return exists

    @staticmethod
    def get(_by, _cols=['product_uuid'], limit=None):
        """ Static method to get Item info

            Params:
            -----
            _by : dict
                Key-value element to query in Product table
            _cols : list
                Columns to retrieve
            limit : int
                Elements to limit query

            Returns:
            -----
            _items : list
                List of elements
        """
        _cols = ','.join(_cols) if _cols else 'product_uuid'
        _where = ' AND '.join(["{} IN {}"\
                                .format(z[0], tuplify(z[1])) \
                            for z in _by.items()]
                        )
        logger.info("Fetching products...")
        _query = "SELECT {} FROM product WHERE {}"\
            .format(_cols, _where)
        if limit:
            _query += ' LIMIT {}'.format(limit)
        logger.debug(_query)
        try:
            _items = g._db.query(_query).fetch()
            logger.debug("Got {} products".format(len(_items)))
        except Exception as e:
            logger.error(e)
            raise errors.ApiError(70003, "Issues fetching elements in DB")
        return _items

    @staticmethod
    def get_one():
        """ Static Method to verify correct connection with Items Postgres DB
        """
        try:
            q = g._db.query("SELECT * FROM product LIMIT 1").fetch()
        except:
            logger.error("Postgres Catalogue Connection error")
            return False
        for i in q:
            logger.info('Product UUID: ' + str(i['product_uuid']))
        return {'msg':'Postgres Items One Working!'}