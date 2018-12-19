from app.models.category import Category
from app.models.attr import Attr
from app.models.nutriment import Nutriment
from app.models.image import ImageProduct
from app.norm.normalize_text import key_format, tuplify
from app.utils import errors
from ByHelpers import applogger
from config import *
from flask import g
import pandas as pd
from sqlalchemy import create_engine, Text
from sqlalchemy.dialects.postgresql import UUID
import datetime
import json

geo_stores_url = 'http://' + SRV_GEOLOCATION + '/store/retailer?key=%s'
logger = applogger.get_logger()


class Product(object):
    """ Class perform insert, update and query methods
        on PSQL Catalogue.product
    """

    __attrs__ = [
        'product_uuid', "product_id", "gtin", "item_uuid",
        "source", "name", "description", "images",
        "categories", "url", "brand", "provider", "attributes",
        "ingredients", "raw_html", "raw_product", "nutriments"
    ]
    __extras__ = ['prod_attrs', 'prod_images', 'prod_categs', 'normalized']
    __base_q = ['product_uuid', 'product_id', 'name', 'source']

    def __init__(self, _args):
        """ Product constructor

            Params:
            -----
            _args : dict
                All arguments to build a `Product` record
        """
        logger.debug("Init product...")
        logger.debug("product: {}".format(str(_args.get('nutriments'))))
        # Arguments verification and addition
        try:
            for _k in self.__attrs__:
                if _k in _args:
                    self.__dict__[_k] = _args[_k]
                    continue
                self.__dict__[_k] = None
        except Exception as e:
            logger.error("Error while creating __attrs__ dict: {}".format(str(e)))

        logger.info("Nutriments!!!   {}".format(_args.get('nutriments')))
        logger.debug("Args ....")
        # Args Aggregation
        try:
            self.gtin = str(self.gtin).zfill(14)[-14:] if self.gtin else None
            self.product_id = str(self.product_id)[-255:] if self.product_id else None
        except Exception as e:
            error_str = "Error while defining identifiers: {}".format(str(e))
            logger.error(error_str)
            if APP_MODE == "CONSUMER":
                raise Exception(error_str)
            if APP_MODE == "SERVICE":
                raise errors.ApiError(70005, error_str)
        # Raw Product construction
        logger.debug("Raw construciton ....")
        try:
            if not self.raw_product:
                self.raw_product = json.dumps(_args)
        except Exception as e:
            logger.error("Cannot serialize product: " + str(e))
            if APP_MODE == "CONSUMER":
                logger.warning("Wrong DataType to serialize for Product!")
                raise Exception("Wrong DataType to serialize for Product!")
            if APP_MODE == "SERVICE":
                raise errors.ApiError(70005, "Wrong DataType to serialize for Product!")
        # Args validation
        logger.debug("Args validation....")
        try:
            if not (isinstance(self.images, list) or (self.images is None)):
                raise Exception("[images] {} instead of list or None".format(type(self.images)))
            if not (isinstance(self.ingredients, list) or (self.ingredients is None)):
                raise Exception("[ingredients] {} instead of list or None".format(type(self.ingredients)))
            if not (isinstance(self.categories, list) or (self.categories is None)):
                raise Exception("[categories] {} instead of list or None".format(type(self.categories)))
            if not (isinstance(self.attributes, dict) or (self.attributes is None)):
                raise Exception("[attributes] {} instead of dict or None".format(type(self.attributes)))
            if not (isinstance(self.nutriments, dict) or (self.nutriments is None)):
                raise Exception("[nutriments] {} instead of dict or None".format(type(self.nutriments)))
            if not (isinstance(self.raw_html, str) or (self.raw_html is None)):
                raise Exception("[raw_html] {} instead of str or None".format(type(self.raw_html)))
        except Exception as e:
            error_str = "WRONG DATATYPE: " + str(e) + "({} {})".format(self.source, self.product_uuid)
            logger.error(error_str)
            if APP_MODE == "CONSUMER":
                raise Exception(error_str)
            if APP_MODE == "SERVICE":
                raise errors.ApiError(70005, error_str)
        logger.debug("Product declaration finished!")

    def save(self, pcommit=True, _is_update=False, verified=False):
        """ Class method to save Product record in DB
            with product_image, product_attr and product_category
        """
        logger.debug("Saving Product...")
        _is_update = False
        # Verify for update
        if self.product_uuid:
            # If already validated for updated, dont do it again
            if _is_update:
                pass
            elif not Product.exists({'product_uuid': self.product_uuid}):
                # If wants to update but wrong UUID, return Error
                if APP_MODE == "CONSUMER":
                    logger.error("Cannot update [{}], UUID not in DB!".format(self.product_uuid))
                    return False
                if APP_MODE == "SERVICE":
                    raise errors.ApiError(70006,
                                          "Cannot update [{}], UUID not in DB!".format(self.product_uuid))
            _is_update = True
        # Verify for insert, if previously verified continue to save
        elif verified:
            pass
        else:
            # If not verified, check if not already in DB
            if Product.exists({'product_id': self.product_id,
                               'source': self.source}):
                self.message = 'Product already exists!'
                self.product_uuid = Product \
                    .get({'product_id': self.product_id,
                          'source': self.source})[0]['product_uuid']
                return True
        # Load model
        logger.debug("Creating __attrs__ dict in product  ...")
        m_prod = g._db.model('product', 'product_uuid')
        aux = {}
        for _k in self.__attrs__:
            if _k not in ['attributes', 'nutriments'] and self.__dict__[_k]:
                m_prod.__dict__[_k] = self.__dict__[_k]

        if m_prod.images:
            m_prod.__dict__['images'] = ', '.join(m_prod.images)

        if m_prod.ingredients:
            m_prod.__dict__['ingredients'] = ', '.join(m_prod.ingredients)

        if m_prod.categories:
            m_prod.__dict__['categories'] = ', '.join(m_prod.categories)
        # Add date
        m_prod.last_modified = str(datetime.datetime.utcnow())
        # Always add what Item UUID is set
        m_prod.item_uuid = str(self.item_uuid) if self.item_uuid else None
        try:

            res = m_prod.save(commit=True)
            self.message = "Correctly {} Product!" \
                .format('updated' if self.product_uuid else 'stored')
            self.product_uuid = m_prod.last_id
            logger.debug(self.message)
            # Save product images
        except Exception as e:
            logger.error("Error saving product: {}".format(str(e)))
            if APP_MODE == "CONSUMER":
                logger.error("Issues saving product in DB!")
                return False
            if APP_MODE == "SERVICE":
                raise errors.ApiError(70002, "Issues saving product in DB!")

        try:
            if self.images:
                self.save_images(pcommit=pcommit)
        except Exception as e:
            if APP_MODE == "CONSUMER":
                logger.error("Issues saving images in DB! {}".format(str(e)))
                return False
            if APP_MODE == "SERVICE":
                raise errors.ApiError(70002, "Issues saving images in DB! {}".format(str(e)))

        try:
            if self.categories:
                self.save_categories(_is_update, pcommit=pcommit)
        except Exception as e:
            logger.error(e)
            if APP_MODE == "CONSUMER":
                logger.error("Issues saving categories in DB! {}".format(str(e)))
                return False
            if APP_MODE == "SERVICE":
                raise errors.ApiError(70002, "Issues saving categories in DB! {}".format(str(e)))

        try:
            self.add_attributes()
            if self.attributes:
                self.save_attributes(_is_update, pcommit=pcommit)
        except Exception as e:
            logger.error(e)
            if APP_MODE == "CONSUMER":
                logger.error("Issues saving attributes in DB! {}".format(str(e)))
                return False
            if APP_MODE == "SERVICE":
                raise errors.ApiError(70002, "Issues saving attributes in DB! {}".format(str(e)))

        try:
            logger.debug("Nutriments: " + str(self.nutriments))
            if self.nutriments:
                self.save_nutriments(_is_update, pcommit=pcommit)
        except Exception as e:
            logger.error(e)
            if APP_MODE == "CONSUMER":
                logger.error("Issues saving nutriments in DB! {}".format(str(e)))
                return False
            if APP_MODE == "SERVICE":
                raise errors.ApiError(70002, "Issues saving nutriments in DB! {}".format(str(e)))

        return True

    def add_attributes(self):
        """ Class method to save brand, provider and categs
            as attributes
        """
        logger.debug("Adding attributes...")
        if not self.attributes:
            self.attributes = {}
        # Load all elements as Attributes
        if self.brand:
            self.attributes['brand'] = [{
                'key': 'brand',
                'name': 'Brand',
                'order': None,
                'qty    ': None,
                'unit': None,
                'value': self.brand
            }]
        if self.provider:
            self.attributes['provider'] = [{
                'key': 'provider',
                'name': 'Provider',
                'order': None,
                'qty    ': None,
                'unit': None,
                'value': self.provider
            }]
        if self.categories:
            categories_attrs = []
            for index, category in enumerate(self.categories):
                categories_attrs.append({
                    'key': 'category',
                    'name': 'Category',
                    'order': index,
                    'qty    ': None,
                    'unit': None,
                    'value': category
                })
            self.attributes['category'] = categories_attrs
        logger.debug("Adding attributes finished... ")

    def save_attributes(self, update=False, pcommit=True):
        """ Class method to save product attributes
        """
        logger.debug("Saving attributes...")
        _nprs = {'key', 'name', 'order', 'qty', 'unit', 'value'}
        logger.debug("Attributes found: {}".format(list(self.attributes.keys())))
        try:
            for key, _attrs in self.attributes.items():
                logger.debug("Iterating attributes dict..")
                for _attr in _attrs:
                    logger.debug("Iterating attrs list..")
                    # Verify if attr exists
                    id_attr = Attr.get_id(_attr['key'], _attr['value'], is_key=True)
                    # If not, create attr
                    logger.debug("Id found: {}".format(id_attr))
                    if not id_attr:
                        attr = Attr({
                            "value": _attr["value"],
                            "clss": {
                                "name": _attr["name"],
                                "key": _attr["key"]
                            }
                        })
                        logger.debug("attr object created...")
                        id_attr = attr.save(commit=pcommit)
                    else:
                        logger.debug("attr already saved")
                    logger.debug("attr saved")
                    # Verify if product_attr exists
                    id_prod_attr = g._db.query("""SELECT id_product_attr
                                                FROM product_attr
                                                WHERE product_uuid = '{}'
                                                AND id_attr = {} LIMIT 1"""
                                               .format(self.product_uuid, id_attr)) \
                        .fetch()
                    # If not create product_attr
                    if id_prod_attr:
                        if not update:
                            logger.debug("Product Attr already in DB!")
                            continue
                        id_prod_attr = id_prod_attr[0]['id_product_attr']
                    # Load model
                    try:
                        m_prod_at = g._db.model('product_attr', 'id_product_attr')
                        if id_prod_attr:
                            m_prod_at.id_product_attr = id_prod_attr
                        m_prod_at.product_uuid = self.product_uuid
                        m_prod_at.id_attr = id_attr
                        if 'qty' in _attr:
                            m_prod_at.qty = _attr['qty']
                        if 'unit' in _attr:
                            m_prod_at.unit = _attr['unit']
                        if 'order' in _attr:
                            m_prod_at.order_ = _attr['order']
                        if 'value' in _attr:
                            m_prod_at.value = _attr['value']
                        m_prod_at.source = self.source
                        m_prod_at.last_modified = str(datetime.datetime.utcnow())
                        m_prod_at.save(commit=pcommit)
                        logger.debug("Product Attr correctly saved! ({})"
                                     .format(m_prod_at.last_id))
                    except Exception as e:
                        logger.error("Error saving attribute in model: {}".format(e))
                        logger.warning("Could not save Product attr!")
                    logger.debug("Saving attributes finished...")
            return True
        except Exception as e:
            logger.error(self.attributes)
            raise Exception("Error in save attributes: {}".format(str(e)))

    def save_nutriments(self, update=False, pcommit=True):
        """ Class method to save product nutriments
        """
        logger.debug("Saving nutriments...")
        _nprs = {'key', 'name', 'qty', 'unit'}
        logger.debug("Nutriments found: {}".format(list(self.nutriments.keys())))
        for key, nutrs in self.nutriments.items():
            logger.debug("Iterating nutriments dict..")
            for nutr in nutrs:
                logger.debug("Iterating nutriment list..")
                # Verify if attr exists
                id_nutriment = Nutriment.get_id(key)
                # If not, create attr
                logger.debug("Id found: {}".format(id_nutriment))
                if not id_nutriment:
                    nutriment = Nutriment({
                        "key": nutr["key"],
                        "name": nutr["name"]
                    })
                    logger.debug("nutriment object created...")
                    id_nutriment = nutriment.save(commit=pcommit)

                logger.debug("nutriment saved")
                # Verify if product_nutriment exists
                id_product_nutriment = g._db.query("""SELECT id_product_nutriment
                                            FROM product_nutriment
                                            WHERE product_uuid = '{}'
                                            AND id_nutriment = {} LIMIT 1"""
                                                   .format(self.product_uuid, id_nutriment)) \
                    .fetch()
                # If not create product_nutriment
                if id_product_nutriment:
                    if not update:
                        logger.debug("Product nutriment already in DB!")
                        continue
                    else:
                        id_product_nutriment = id_product_nutriment[0]['id_product_nutriment']

                # Load model
                try:
                    m_prod_nutr = g._db.model('product_nutriment', 'id_product_nutriment')
                    if id_product_nutriment:
                        m_prod_nutr.id_product_nutriment = id_product_nutriment
                    m_prod_nutr.product_uuid = self.product_uuid
                    m_prod_nutr.id_nutriment = id_nutriment
                    if 'qty' in nutr:
                        try:
                            if nutr['qty'] is None:
                                m_prod_nutr.qty = None
                            else:
                                m_prod_nutr.qty = int(nutr['qty'])
                        except Exception as e:
                            logger.error("Error in qty nutriment: {}".format(str(e)))
                            raise Exception("Error casting qty nutriment")

                    if 'unit' in nutr:
                        m_prod_nutr.unit = nutr['unit']
                    m_prod_nutr.source = self.source
                    m_prod_nutr.last_modified = str(datetime.datetime.utcnow())
                    m_prod_nutr.save(commit=pcommit)
                    logger.debug("Product Nutriment correctly saved! ({})"
                                 .format(m_prod_nutr.last_id))
                except Exception as e:
                    logger.error(e)
                    logger.warning("Could not save Product Nutriments!")
                logger.debug("Saving Nutriments finished...")
        return True

    def save_images(self, pcommit=True):
        """ Class method to save product images
        """
        prod = {
            'product_uuid': self.product_uuid,
            'images': self.images
        }
        images_sorted = ImageProduct.download_img(prod)
        if images_sorted:
            for image in images_sorted:
                try:

                    _exist = g._db.query("""SELECT id_product_image FROM product_image
                                                                        WHERE product_uuid = '{}'
                                                                        AND image = '{}'"""
                                         .format(image.get('product_uuid'), image.get('image'))) \
                        .fetch()
                    if _exist:
                        continue

                    feature = ImageProduct.generate_features(image['content'])
                    logger.debug("Feature obtained [{}]".format(type(feature)))
                    product_image = g._db.model('product_image', 'id_product_image')
                    product_image.descriptor = json.dumps(feature.tolist())
                    product_image.product_uuid = image.get('product_uuid')
                    product_image.image = image.get('image')
                    product_image.last_modified = str(datetime.datetime.utcnow())
                    product_image.save(commit=pcommit)
                    logger.debug("Product Image correctly saved! ({})"
                                 .format(product_image.last_id))
                except Exception as e:
                    logger.error("Could't save Product image: {}".format(str(e)))
                    continue
        else:
            logger.warning("Cannot download any image from {}".format(self.product_uuid))
        return True

    @staticmethod
    def undo_match(puuid):
        """ Method to undo matching, by setting Item UUID
            of a given product to NULL
        """
        try:
            g._db.query("""UPDATE product
                SET item_uuid = NULL
                WHERE product_uuid = '{}'
                """.format(puuid))
            return {
                'status': 'OK',
                'msg': 'Product ({}) correctly reset!'.format(puuid)
            }
        except Exception as e:
            logger.error(e)
            raise errors.ApiError(70003, "Issues updating elements in DB")

    @staticmethod
    def update_image(p_obj, or_create=False):
        """ Static method to update a product image

            Params:
            -----
            p_obj : dict
                Product Image details (product_uuid, image, descriptors) 
        """
        try:
            # Verify if prod image exists
            id_pimg = g._db.query("""SELECT id_product_image
                    FROM product_image
                    WHERE product_uuid = '{}'
                    AND image = '{}'
                    LIMIT 1""" \
                                  .format(p_obj['product_uuid'], p_obj['image'])) \
                .fetch()
            if not id_pimg:
                if not or_create:
                    logger.warning("Cannot update, image not in DB!")
                    if APP_MODE == "CONSUMER":
                        return False
                    if APP_MODE == "SERVICE":
                        raise errors \
                            .ApiError(70006, "Cannot update, image not in DB!")
                id_pimg = None
            id_pimg = id_pimg[0]['id_product_image']
            # Load model
            Product.save_pimage(p_obj['product_uuid'],
                                p_obj['image'], id_pimg,
                                p_obj['descriptor']
                                if 'descriptor' in p_obj
                                else [])
            return {'message': 'Product Image correctly updated!'}
        except Exception as e:
            logger.error(e)
            logger.warning("Could not save Product image!")
            if APP_MODE == "CONSUMER":
                return False
            if APP_MODE == "SERVICE":
                raise errors \
                    .ApiError(70004, "Could not apply transaction in DB")

    def save_categories(self, update=False, pcommit=True):
        """ Class method to save product categories
        """
        logger.debug("Saving categories...")
        _parent = None
        id_parent = None
        for _cat in self.categories:
            try:
                # Get ID if exists, otherwise create category
                id_cat = Category.get_id(_cat, self.source)
                if not id_cat:
                    categ = Category({
                        'source': self.source,
                        'id_parent': id_parent,
                        'name': _cat
                    })
                    id_parent = categ.save(commit=pcommit)
                    # Emergency skip
                    if not id_parent:
                        raise Exception("Cannot get id while saving category")
                else:
                    id_parent = id_cat
                # Verify product category does not exist
                id_prod_categ = g._db.query("""SELECT id_product_category
                                            FROM product_category
                                            WHERE id_category = {}
                                            AND product_uuid = '{}' LIMIT 1"""
                                            .format(id_parent,
                                                    self.product_uuid)) \
                    .fetch()
                if id_prod_categ:
                    if not update:
                        logger.info("Category already assigned to Product!")
                        continue
                    id_prod_categ = id_prod_categ[0]['id_product_category']
                m_prod_cat = g._db.model('product_category',
                                         'id_product_category')
                if id_prod_categ:
                    m_prod_cat.id_product_category = id_prod_categ
                m_prod_cat.product_uuid = self.product_uuid
                m_prod_cat.id_category = id_parent
                m_prod_cat.last_modified = str(datetime.datetime.utcnow())
                m_prod_cat.save(commit=pcommit)
                logger.debug("Product Category correctly saved! ({})"
                             .format(m_prod_cat.last_id))
            except Exception as e:
                logger.error(e)
                logger.warning("Could not save Product category!")
        logger.debug("Saving categories finished...")
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
        _where = ' AND '.join(["{}='{}'".format(*z)
                               for z in list(k_param.items())])
        try:
            _q = """SELECT EXISTS (SELECT 1 FROM product WHERE {} LIMIT 1)""" \
                .format(_where)
            logger.debug("Query: {}".format(_q))
            exists = g._db.query(_q) \
                .fetch()[0]['exists']
        except Exception as e:
            logger.error(e)
            return False
        return exists

    @staticmethod
    def puuid_from_cache(cached_ps, _p):
        """ Static method to verify elements from cached products

            Params:
            -----
            cached_ps: dict
                Nested elements with keys of retailers and its ids
            _p : Product

            Returns:
            -----
            puuid : str
                Product UUID or None
        """
        if _p.source in cached_ps.keys():
            if _p.product_id in cached_ps[_p.source]:
                return [{'product_uuid': cached_ps[_p.source][_p.product_id]}]
        return None

    @staticmethod
    def create_cache_ids():
        """ Static method to initialize cache ids

            Returns:
            -----
            cache_ids : dict
                Nested dict by source and product_id to product_uuid map
        """
        _df = pd \
            .read_sql("""SELECT product_uuid, source, product_id
                    FROM product WHERE source NOT IN ('ims','plm','nielsen','gs1')""",
                      g._db.conn)
        cache_ids = {}
        for y, gdf in _df.groupby('source'):
            cache_ids[y] = gdf[['product_uuid', 'product_id']] \
                .set_index('product_id') \
                .to_dict()['product_uuid']
        # Clean GC
        del _df
        return cache_ids

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
        _where = ' AND '.join(["{} IN {}"
                              .format(z[0], tuplify(z[1]))
                               for z in _by.items()])
        logger.debug("Fetching products...")
        _query = "SELECT {} FROM product WHERE {}" \
            .format(_cols, _where)
        if limit:
            _query += ' LIMIT {}'.format(limit)
        logger.debug(_query)
        try:
            _items = g._db.query(_query).fetch()
            logger.debug("Got {} products".format(len(_items)))
        except Exception as e:
            logger.error(e)
            if APP_MODE == "CONSUMER":
                logger.error("Issues fetching elements in DB!")
                return False
            if APP_MODE == "SERVICE":
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
            logger.debug('Product UUID: ' + str(i['product_uuid']))
        return {'msg': 'Postgres Items One Working!'}

    @staticmethod
    def query(_by, **kwargs):
        """ Static method to query by defined column values

            Params:
            -----
            _by : str
                Key from which query is performed
            kwargs : dict
                Extra arguments, such as (p, ipp, cols, etc..)
            
            Returns:
            -----
            _resp : list
                List of product objects
        """
        logger.debug('Querying by: {}'.format(_by))
        logger.debug('fetching: {}'.format(kwargs))
        # Format columns
        if kwargs['cols']:
            _cols = ','.join([x for x in \
                              (kwargs['cols'].split(',') \
                               + Product.__base_q) \
                              if x in Product.__attrs__])
        else:
            _cols = ','.join([x for x in Product.__base_q])
        # Format querying keys
        if kwargs['keys']:
            _keys = 'WHERE ' + _by + ' IN ' + str(tuplify(kwargs['keys']))
        else:
            if _by != 'product_uuid':
                _keys = 'WHERE {} IS NULL'.format(_by)
            else:
                _keys = ''
        # Format paginators
        _p = int(kwargs['p'])
        if _p < 1:
            _p = 1
        _ipp = int(kwargs['ipp'])
        if _ipp > 10000:
            _ipp = 10000
        # Order by statement
        if 'orderby' in kwargs:
            _orderby = kwargs['orderby'] if kwargs['orderby'] else 'product_uuid'
        else:
            _orderby = 'product_uuid'
        if _orderby not in Product.__base_q:
            _orderby = 'product_uuid'
        # Build query
        _qry = """SELECT {} FROM product {} ORDER BY {} OFFSET {} LIMIT {} """ \
            .format(_cols, _keys, _orderby, (_p - 1) * _ipp, _ipp)
        logger.debug(_qry)
        # Query DB
        try:
            _resp = g._db.query(_qry).fetch()
            logger.debug("Found {} products".format(len(_resp)))
        except Exception as e:
            logger.error(e)
            logger.warning("Issues fetching elements in DB!")
            if APP_MODE == "CONSUMER":
                return False
            if APP_MODE == "SERVICE":
                raise errors.ApiError(70003, "Issues fetching elements in DB")
        # Verify for additional cols
        extra_cols = [x for x in (set(kwargs['cols'].split(',')) \
                                  - set(_cols)) if x in Product.__extras__]
        if extra_cols and _resp:
            p_uuids = [_u['product_uuid'] for _u in _resp]
            _extras = Product.fetch_extras(p_uuids, extra_cols)
        # Aggregate extra columns
        for _i, _r in enumerate(_resp):
            _tmp_extras = {}
            for _ex in extra_cols:
                _tmp_extras.update({
                    _ex: _extras[_r['product_uuid']][_ex]
                })
            _resp[_i].update(_tmp_extras)
        return _resp

    @staticmethod
    def fetch_extras(p_uuids, _cols):
        """ Static method to retrieve foreign references

            Params:
            -----
            p_uuids : list
                List of Product UUIDs
            _cols : list
                Additional columns to retrieve
            
            Returns:
            -----
            _prod_ext : dict
                Dict with product extras.
            >>> {
                "8z46-df4as6df4-af4asdf9": {
                    "prod_images": [
                        {
                            "id_p_image": 45596,
                            "image": "Medicamentos de Patente",
                            "descriptor": [[0,2,3,1,4,5], [3,4,6,7,7]],
                            "last_mod": "2018-01-03"
                        }, # ...
                    ], 
                    "prod_attrs": [
                        {
                            "id_p_attr": 485,
                            "value": 80,
                            "attr": "Miligramos",
                            "clss": "Presentación"
                        }, # ...
                    ],
                    "prod_categs": [
                        {
                            "id_p_cat": 75,
                            "code": "SD20",
                            "cat": "Medicamentos"
                        }, # ...
                    ]
                }
            }
                (key -> Prod UUID, values -> {'col': <list of attrs>})
        """
        # Initialize prod_ext dict
        _prod_ext, p_extrs = {}, {}
        for z in p_uuids:
            _prod_ext[z] = {w: [] for w in _cols}
        if 'prod_attrs' in _cols:
            # Fetch Product Attrs
            logger.debug('Retrieving Product Attrs...')
            p_extrs['prod_attrs'] = Product.query_attrs(p_uuids)
        if 'prod_images' in _cols:
            # Fetch Product Images
            logger.debug('Retrieving Product Images...')
            p_extrs['prod_images'] = Product.query_imgs(p_uuids)
        if 'prod_categs' in _cols:
            # Fetch Product Images
            logger.debug('Retrieving Product Categories...')
            p_extrs['prod_categs'] = Product.query_categs(p_uuids)
        if 'normalized' in _cols:
            # Fetch Product Normalized text
            logger.info('Retrieving Product Normalized...')
            p_extrs['normalized'] = Product.query_normed(p_uuids)
        # Add attrs, imgs, normed and categs to complete dict
        for _p in _prod_ext:
            # Loop over available columns
            for _cl in _cols:
                if _p in p_extrs[_cl]:
                    if not _prod_ext[_p][_cl]:
                        _prod_ext[_p][_cl] = p_extrs[_cl][_p]
                    else:
                        _prod_ext[_p][_cl].append(p_extrs[_cl][_p])
        logger.debug(_prod_ext)
        return _prod_ext

    @staticmethod
    def query_attrs(p_uuids):
        """ Fetch all attributes by Prod UUID

            Params:
            -----
            p_uuids : list
                List of Product UUIDs
            
            Returns:
            -----
            p_attrs : dict
                Product Attributes hashed by Product UUID
        """
        p_attrs = {}
        _qry = """SELECT pat.product_uuid, 
            pat.id_product_attr as id_p_attr, pat.value,
            to_char(pat.last_modified, 'YYYY-MM-DD HH24:00:00') as last_mod, 
            att.name as attr, att.name_es as clss 
            FROM product_attr pat 
            LEFT OUTER JOIN (
                SELECT id_attr, attr.name, clss.name_es
                FROM attr INNER JOIN clss
                ON (clss.id_clss = attr.id_clss)) AS att
            ON (att.id_attr = pat.id_attr)
            WHERE product_uuid IN {}
            ORDER BY product_uuid
            """.format(tuplify(p_uuids))
        logger.debug(_qry)
        try:
            resp_at = g._db.query(_qry).fetch()
            for _rat in resp_at:
                _pu = _rat['product_uuid']
                del _rat['product_uuid']
                if _pu in p_attrs:
                    p_attrs[_pu].append(_rat)
                else:
                    p_attrs[_pu] = [_rat]
        except Exception as e:
            logger.error(e)
            logger.warning("Issues fetching Product Attrs!")
            return {}
        logger.debug('Found {} attributes'.format(len(p_attrs)))
        logger.debug(p_attrs)
        return p_attrs

    @staticmethod
    def query_normed(p_uuids):
        """ Fetch all normalized text by Prod UUID

            Params:
            -----
            p_uuids : list
                List of Product UUIDs
            
            Returns:
            -----
            p_norm : dict
                Product Normalized text hashed by Product UUID
        """
        p_norm = {}
        for i in range(0, len(p_uuids), 1000):
            _qry = """SELECT product_uuid, normalized
                FROM product_normalized
                WHERE product_uuid IN {}
                """.format(tuplify(p_uuids[i: i + 1000]))
            logger.debug(_qry)
            try:
                resp_norm = g._db.query(_qry).fetch()
                for _rnom in resp_norm:
                    _pu = _rnom['product_uuid']
                    del _rnom['product_uuid']
                    p_norm[_pu] = _rnom['normalized']
            except Exception as e:
                logger.error(e)
                logger.warning("Issues fetching Product Normalized!")
                continue
        logger.info("Found {} normed texts".format(len(p_norm)))
        return p_norm

    @staticmethod
    def query_imgs(p_uuids):
        """ Fetch all images by Prod UUID

            Params:
            -----
            p_uuids : list
                List of Product UUIDs
            
            Returns:
            -----
            p_imgs : dict
                Product Images hashed by Product UUID
        """
        p_imgs = {}
        _qry = """SELECT product_uuid, 
            id_product_image as id_p_image,
            image, descriptor,
            to_char(last_modified, 'YYYY-MM-DD HH24:00:00') as last_modified
            FROM product_image
            WHERE product_uuid IN {}
            ORDER BY product_uuid
            """.format(tuplify(p_uuids))
        logger.debug(_qry)
        try:
            resp_im = g._db.query(_qry).fetch()
            for _rim in resp_im:
                _pu = _rim['product_uuid']
                del _rim['product_uuid']
                if _pu in p_imgs:
                    p_imgs[_pu].append(_rim)
                else:
                    p_imgs[_pu] = [_rim]
        except Exception as e:
            logger.error(e)
            logger.warning("Issues fetching Product Images!")
            return {}
        logger.debug('Found {} images'.format(len(p_imgs)))
        logger.debug(p_imgs)
        return p_imgs

    @staticmethod
    def query_categs(p_uuids):
        """ Fetch all catgories by Prod UUID

            Params:
            -----
            p_uuids : list
                List of Product UUIDs
            
            Returns:
            -----
            p_categs : dict
                Product Categories hashed by Product UUID
        """
        p_categs = {}
        _qry = """SELECT pca.product_uuid,
            pca.id_product_category as id_p_attr,
            to_char(pca.last_modified, 'YYYY-MM-DD HH24:00:00') as last_mod,
            ca.name as cat, ca.code
            FROM product_category pca
            LEFT OUTER JOIN category ca
            ON (ca.id_category = pca.id_category)
            WHERE product_uuid IN {}
            ORDER BY product_uuid
            """.format(tuplify(p_uuids))
        logger.debug(_qry)
        try:
            resp_ca = g._db.query(_qry).fetch()
            for _rca in resp_ca:
                _pu = _rca['product_uuid']
                del _rca['product_uuid']
                if _pu in p_categs:
                    p_categs[_pu].append(_rca)
                else:
                    p_categs[_pu] = [_rca]
        except Exception as e:
            logger.error(e)
            logger.warning("Issues fetching Product Categs!")
            return {}
        logger.debug('Found {} categories'.format(len(p_categs)))
        logger.debug(p_categs)
        return p_categs

    @staticmethod
    def filter(field, valu, **kwargs):
        """ Retrieve by defined filter

            Params:
            -----
            field : str
                Attribute key to perform filter by
            valu : str
                Values to perform filter by
            kwargs : dict
                Extra arguments such as (p, ipp, values, etc..)
            
            Returns:
            -----
            _resp : list
                List of product objects
        """
        # Format querying keys
        if kwargs['keys']:
            _keys = ' WHERE ' + field + ' IN ' \
                    + str(tuplify([key_format(z)
                                   for z in kwargs['keys'].split(',')]))
        else:
            _keys = ' WHERE {} IS NULL'.format(_by)
        # Format values if available
        if kwargs['vals']:
            _vals = ' AND ' + valu + ' IN ' \
                    + str(tuplify([y for y in kwargs['vals'].split(',')]))
        else:
            _vals = ''
        # Format retailers if available
        if kwargs['rets']:
            _rets = ' AND ' + field.split('.')[0] \
                    + '.source IN ' \
                    + str(tuplify([y for y in kwargs['rets'].split(',')]))
        else:
            _rets = ''
        # Build filter query
        f_query = """SELECT {ref_table}.product_uuid FROM {table}
            INNER JOIN {ref_table}
            ON ({table}.id_{table} = {ref_table}.id_{table})
            {keys} {vals} {rets} """ \
            .format(table=field.split('.')[0],
                    ref_table=valu.split('.')[0],
                    keys=_keys,
                    vals=_vals,
                    rets=_rets)
        logger.debug(f_query)
        try:
            _fres = g._db.query(f_query).fetch()
            if not _fres:
                return []
            logger.debug("Found {} prods by filters"
                         .format(len(_fres)))
        except Exception as e:
            logger.error(e)
            logger.warning("Could not fetch Filters!")
            return []
        # Return Product query response
        kwargs.update({
            'keys': ','.join([_o['product_uuid'] for _o in _fres])
        })
        return Product.query('product_uuid', **kwargs)

    @staticmethod
    def delete_extra(_uuid, _id, _table):
        """ Static method to delete Product reference table

            Params:
            -----
            _uuid : str
                Product UUID of respective Prod Image
            _id : str
                Product Extra ID to delete
            _table : str
                Table name to delete from

            Returns:
            -----
            resp : bool
                Transaction status
        """
        try:
            _exists = g._db.query("""SELECT EXISTS (
                                    SELECT 1 FROM {table}
                                    WHERE product_uuid = '{uuid}'
                                    AND id_{table} = {_id}
                                    )"""
                                  .format(table=_table,
                                          uuid=_uuid,
                                          _id=_id)) \
                .fetch()[0]['exists']
            if not _exists:
                return {
                    'message': "Product Image ID not in DB!"
                }
            # Delete from Product extra record
            g._db.query("""DELETE FROM {table}
                        WHERE product_uuid='{uuid}'
                        AND id_{table}={_id}"""
                        .format(table=_table,
                                uuid=_uuid,
                                _id=_id))
        except Exception as e:
            logger.error(e)
            raise errors.ApiError(70004, "Could not apply transaction in DB")
        return {
            'message': "Product Extra ({}) correctly deleted!".format(_id)
        }

    @staticmethod
    def delete(p_uuid):
        """ Static method to delete Product

            Params:
            -----
            p_uuid : str
                Product UUID to deletedelete

            Returns:
            -----
            resp : bool
                Transaction status
        """
        logger.debug("Deleting Product...")
        if not Product.exists({'product_uuid': p_uuid}):
            return {
                'message': "Product UUID not in DB!"
            }
        try:
            # Delete from Product image
            g._db.query("DELETE FROM product_image WHERE product_uuid='{}'"
                        .format(p_uuid))
            # Delete from Product Category
            g._db.query("DELETE FROM product_category WHERE product_uuid='{}'"
                        .format(p_uuid))
            # Delete from Product Attr
            g._db.query("DELETE FROM product_attr WHERE product_uuid='{}'"
                        .format(p_uuid))
            # Delete from Product
            g._db.query("DELETE FROM product WHERE product_uuid='{}'"
                        .format(p_uuid))
        except Exception as e:
            logger.error(e)
            if APP_MODE == 'SERVICE':
                raise errors \
                    .ApiError(70004, "Could not apply transaction in DB")
            else:
                return False
        return {
            'message': "Product ({}) correctly deleted!".format(p_uuid)
        }

    @staticmethod
    def upload_normalized(csvfile, _ifexists='replace'):
        """ Static method to batch load into
            product normalized table.

            Params:
            -----
            csvfile : werkzeug.datastructures.FileStorage
                CSV File with product UUID and normed text
            _ifexists : str
                Method to apply if exists (append | replace)

            Returns:
            -----
            flag : dict
                Feedback of saving status
        """
        # Verify file
        try:
            df = pd.read_csv(csvfile)
            logger.debug("Received {} products to upload".format(len(df)))
        except Exception as e:
            logger.error(e)
            logger.warning("Could not read CSV file")
            raise errors.ApiError(70008, "Could not read attached file")
        # Validate columns
        if not set({'product_uuid', 'normalized'}) \
                .issubset(set(df.columns.tolist())):
            logger.debug(df.columns.tolist())
            logger.warning("Missing columns!")
            raise errors.ApiError(70009, "Wrong file format!")
        try:
            _eng = create_engine("postgresql://{}:{}@{}:{}/{}"
                                 .format(SQL_USER,
                                         SQL_PASSWORD,
                                         SQL_HOST,
                                         SQL_PORT,
                                         SQL_DB))
            logger.info("Storing {} products..".format(len(df)))
            df[['product_uuid', 'normalized']] \
                .set_index('product_uuid') \
                .to_sql('product_normalized', _eng,
                        dtype={'product_uuid': UUID, 'normalized': Text},
                        if_exists=_ifexists, chunksize=5000)
        except Exception as e:
            logger.error(e)
            return {'status': 'ERROR',
                    'message': 'Issues upserting normalized names'}
        logger.debug("Finished upserting table")
        return {'status': 'OK',
                'message': 'Correctly upserted normalized names!'}

    @staticmethod
    def intersection(**kwargs):
        """ Query products by intersection of one
            or various cols
        """
        p = int(kwargs['p'])
        ipp = int(kwargs['ipp'])
        del kwargs['p'], kwargs['ipp']

        # Columns
        if 'cols' not in kwargs or not kwargs['cols']:
            cols = ["*"]
        else:
            cols = kwargs['cols']
            del kwargs['cols']

        where = []
        where_qry = """ """
        for k, vals in kwargs.items():
            where.append(" {} IN ({}) ".format(k, vals))

        if where:
            where_qry = """ where {}""".format(""" and """.join(where))

        # Query
        qry = """
            select {} from product {}
            limit {}
            offset {}
        """.format(
            """, """.join(cols),
            where_qry,
            ipp,
            (p - 1) * ipp
        )

        try:
            rows = g._db.query(qry).fetch()
        except Exception as e:
            logger.error("Could not execute intersect query: {}".format(qry))
            raise errors.ApiError(70007, "Could not execute query: ")

        return prods
