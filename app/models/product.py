import datetime
from flask import g
from app.utils import errors, applogger
from config import *
import requests
from pprint import pformat as pf
import ast
import json
from app.models.category import Category
from app.models.attr import Attr
from app.norm.normalize_text import key_format, tuplify

geo_stores_url = 'http://'+SRV_GEOLOCATION+'/store/retailer?key=%s'
logger = applogger.get_logger()

class Product(object):
    """ Class perform insert, update and query methods 
        on PSQL Catalogue.product 
    """

    __attrs__ = ['product_uuid', "product_id", "gtin", "item_uuid",
        "source", "name", "description", "images", "normalized",
        "categories", "url", "brand", "provider", "attributes",
        "ingredients","raw_html", "raw_product"]
    
    __extras__ = ['prod_attrs', 'prod_images', 'prod_categs']

    __base_q = ['product_uuid', 'product_id', 'name', 'source']
    
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
        _is_update = False
        # Verify for update
        if self.product_uuid:
            if not Product.exists({'product_uuid': self.product_uuid}):
                # If wants to update but wrong UUID, return Error                
                raise errors.ApiError(70006, "Cannot update, UUID not in DB!")
            _is_update = True
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
                self.save_images(_is_update)
            # Save product categories
            if self.categories:
                self.save_categories(_is_update)
            # Save product attrs
            if self.attributes:
                self.save_attributes(_is_update)
            # Save category, brand and provider as attributes            
            self.save_extras(_is_update)
        except Exception as e:
            logger.error(e)
            raise errors.ApiError(70002, "Issues saving in DB!")
        return True

    def save_extras(self, update=False):
        """ Class method to save brand, provider and categs 
            as attributes
        """
        self.attributes = []
        # Load all elements as Attributes
        if self.brand:
            self.attributes.append({
                'attr_name': self.brand,
                'attr_key': key_format(self.brand),
                'clss_name': 'Marca',
                'clss_key': 'brand',
                'clss_desc': 'Marca'
            })
        if self.provider:
            self.attributes.append({
                'attr_name': self.provider,
                'attr_key': key_format(self.provider),
                'clss_name': 'Proveedor',
                'clss_key': 'provider',
                'clss_desc': 'Proveedor, Laboratorio, Manufacturador, etc.'
            })
        for _c in self.categories.split(','):
            self.attributes.append({
                'attr_name': _c,
                'attr_key': key_format(_c),
                'clss_name': 'Categoría',
                'clss_key': 'category',
                'clss_desc': 'Categoría'
            })
        self.save_attributes(update)

    def save_attributes(self, update=False):
        """ Class method to save product attributes
        """
        _nprs = {'attr_name', 'clss_name', 'attr_key', 'clss_key'}
        for _attr in self.attributes:
            # Validate attrs
            if not _nprs.issubset(_attr.keys()):
                logger.warning("Cannot add product attribute, missing keys!")
                continue
            # Verify if attr exists
            id_attr = Attr.get_id(_attr['attr_name'], self.source)
            # If not, create attr
            if not id_attr:
                attr = Attr({
                    'name': _attr["attr_name"],
                    "key": _attr["attr_key"],
                    "has_value": 1 if "value" in _attr else 0,
                    "source": self.source,
                    "clss": {
                        "name_es": _attr["clss_name"],
                        "key": _attr["clss_key"],
                        "description": _attr["clss_desc"] if "clss_desc" in _attr else None,
                        "source": self.source
                    }
                })
                id_attr = attr.save()
            # Verify if product_attr exists
            id_prod_attr = g._db.query("""SELECT id_product_attr
                        FROM product_attr
                        WHERE product_uuid = '{}'
                        AND id_attr = {} LIMIT 1"""\
                        .format(self.product_uuid, id_attr))\
                    .fetch()
            # If not create product_attr
            if id_prod_attr:
                if not update:
                    logger.info("Product Attr already in DB!")
                    continue
                id_prod_attr = id_prod_attr[0]['id_product_attr']
            # Load model
            try:
                m_prod_at = g._db.model('product_attr', 'id_product_attr')
                if id_prod_attr:
                    m_prod_at.id_product_attr = id_prod_attr
                m_prod_at.product_uuid = self.product_uuid
                m_prod_at.id_attr = id_attr
                if 'value' in _attr:
                    m_prod_at.value = _attr['value']
                if 'precision' in _attr:
                    m_prod_at.precision = _attr['precision']
                m_prod_at.last_modified = str(datetime.datetime.utcnow())
                m_prod_at.save()
                logger.info("Product Attr correctly saved! ({})"\
                    .format(m_prod_at.last_id))
            except Exception as e:
                logger.error(e)
                logger.warning("Could not save Product attr!")
        return True

    def save_images(self, update=False):
        """ Class method to save product images
        """
        for _img in self.images:
            try:
                # Verify if prod image exists
                _exist = g._db.query("""SELECT EXISTS (
                        SELECT 1 FROM product_image
                        WHERE product_uuid = '{}'
                        AND image = '{}')"""\
                        .format(self.product_uuid, _img))\
                    .fetch()[0]['exists']
                if _exist:
                    if update:
                        Product.update_image({
                            'product_uuid': self.product_uuid,
                            'image': _img
                        }, True)
                    else:
                        logger.info("Image already in DB!")
                    continue
                # Load model
                Product.save_pimage(self.product_uuid, _img)
            except Exception as e:
                logger.error(e)
                logger.warning("Could not save Product image!")
        return True
    
    @staticmethod
    def save_pimage(p_uuid, _img, id_pim=None, descs=[]):
        """ Static method to store in `product image`
        """
        m_prod_im = g._db.model('product_image', 'id_product_image')
        if id_pim:
            m_prod_im.id_product_image = id_pim
        m_prod_im.product_uuid = p_uuid
        m_prod_im.image = _img
        if descs:
            m_prod_im.descriptor = json.dumps(descs)
        m_prod_im.last_modified = str(datetime.datetime.utcnow())
        m_prod_im.save()
        logger.info("Product Image correctly saved! ({})"\
            .format(m_prod_im.last_id))
        return True
    
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
                    LIMIT 1"""\
                    .format(p_obj['product_uuid'], p_obj['image']))\
                .fetch()
            if not id_pimg:
                if not or_create:
                    logger.info("Cannot update, image not in DB!")
                    raise errors.ApiError(70006, "Cannot update, image not in DB!")
                id_pimg = None
            id_pimg = id_pimg[0]['id_product_image']
            # Load model
            Product.save_pimage(p_obj['product_uuid'],
                                p_obj['image'], id_pimg,
                                p_obj['descriptor'] if 'descriptor' in p_obj \
                                                else [])
            return {'message': 'Product Image correctly updated!'}
        except Exception as e:
            logger.error(e)
            logger.warning("Could not save Product image!")
            raise errors.ApiError(70004, "Could not apply transaction in DB")
    
    def save_categories(self, update=False):
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
                id_prod_categ = g._db.query("""SELECT id_product_category FROM product_category
                        WHERE id_category = {}
                        AND product_uuid = '{}' LIMIT 1"""\
                        .format(id_cat, self.product_uuid))\
                        .fetch()
                if id_prod_categ:
                    if not update:
                        logger.info("Category already assigned to Product!")
                        continue
                    id_prod_categ = id_prod_categ[0]['id_product_category']
                m_prod_cat = g._db.model('product_category', 'id_product_category')
                if id_prod_categ:
                    m_prod_cat.id_product_category = id_prod_categ
                m_prod_cat.product_uuid = self.product_uuid
                m_prod_cat.id_category = id_cat
                m_prod_cat.last_modified = str(datetime.datetime.utcnow())
                m_prod_cat.save()
                logger.info("Product Category correctly saved! ({})"\
                    .format(m_prod_cat.last_id))
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

    @staticmethod
    def delete(p_uuid):
        """ Static method to delete Product

            Params:
            -----
            p_uuid : str
                Product UUID to delete

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
            g._db.query("DELETE FROM product_image WHERE product_uuid='{}'"\
                        .format(p_uuid))
            # Delete from Product Category
            g._db.query("DELETE FROM product_category WHERE product_uuid='{}'"\
                        .format(p_uuid))
            # Delete from Product Attr
            g._db.query("DELETE FROM product_attr WHERE product_uuid='{}'"\
                        .format(p_uuid))
            # Delete from Product
            g._db.query("DELETE FROM product WHERE product_uuid='{}'"\
                        .format(p_uuid))
        except Exception as e:
            logger.error(e)
            raise errors.ApiError(70004, "Could not apply transaction in DB")
        return {
            'message': "Product ({}) correctly deleted!".format(p_uuid)
        }
    
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
        # Format params
        if kwargs['cols']:
            _cols = ','.join([x for x in \
                            (kwargs['cols'].split(',') \
                            + Product.__base_q) \
                        if x in Product.__attrs__])
        if kwargs['keys']:
            _keys = 'WHERE ' + _by + ' IN ' + str(tuplify(kwargs['keys']))
        else:
            _keys = 'WHERE {} IS NULL'.format(_by)
        _p = int(kwargs['p'])
        if _p < 1 :
            _p = 1
        _ipp = int(kwargs['ipp'])
        # Build query
        _qry = """SELECT {} FROM product {} OFFSET {} LIMIT {} """\
            .format(_cols, _keys, (_p - 1)*_ipp, _ipp)
        logger.debug(_qry)
        # Query DB
        try:
            _resp = g._db.query(_qry).fetch()
            logger.info("Found {} products".format(len(_resp)))
        except Exception as e:
            logger.error(e)
            logger.warning("Issues fetching elements in DB")
            raise errors.ApiError(70003, "Issues fetching elements in DB")
        # Verify for additional cols
        extra_cols = [x for x in (set(kwargs['cols'].split(',')) \
                        -  set(_cols)) if x in Product.__extras__]
        if extra_cols and _resp:
            p_uuids = [_u['product_uuid'] for _u in _resp]
            _extras = Product.fetch_extras(p_uuids, extra_cols)
        # Aggregate extra columns
        for _i, _r in enumerate(_resp):
            _tmp_extras = {}
            for _ex in extra_cols:
                _tmp_extras.update({
                    _ex : _extras[_r['product_uuid']][_ex]
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
            _prod_ext[z]= {w:[] for w in _cols}
        if 'prod_attrs' in _cols:
            # Fetch Product Attrs
            logger.info('Retrieving Product Attrs...')
            p_extrs['prod_attrs'] = Product.query_attrs(p_uuids)
        if 'prod_images' in _cols:
            # Fetch Product Images
            logger.info('Retrieving Product Images...')
            p_extrs['prod_images'] = Product.query_imgs(p_uuids)
        if 'prod_categs' in _cols:
            # Fetch Product Images
            logger.info('Retrieving Product Categories...')
            p_extrs['prod_categs'] = Product.query_categs(p_uuids)
        # Add attrs, imgs and categs to complete dict
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
        logger.info('Found {} attributes'.format(len(p_attrs)))
        logger.debug(p_attrs)
        return p_attrs
    
    @staticmethod
    def query_imgs(p_uuids):
        """ Fetch all images by Prod UUID

            Params:
            -----
            p_uuids : list
                List of Product UUIDs
            _cols : list
                Additional columns to retrieve
            
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
        logger.info('Found {} images'.format(len(p_imgs)))
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
        logger.info('Found {} categories'.format(len(p_categs)))
        logger.debug(p_categs)
        return p_categs