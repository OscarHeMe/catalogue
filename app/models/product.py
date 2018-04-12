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
<<<<<<< HEAD
        Class perform Query methods on PostgreSQL items
    """
    product_uuid = None
    item_uuid = None # REFERENCES "item" (item_uuid),
    source = None
    retailer = None
    product_id = None
    id = None
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
    date = None

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
        req_vars = ["retailer", "id", "name"]
        keys = list(elem.keys())
        # Si no tiene todas las keys requeridas regresamos False
        if not set(req_vars).issubset(keys):
            return False
        return True

    @property
    def as_dict(self):
        ''' Dictionary representation for saving to cassandra '''
        return {
            'product_id' : str(self.id),
            'name' : str(self.name),
            'gtin' : str(self.gtin) if self.gtin else None,
            'description' : str(self.description) if self.description else None,
            'raw_product' : str(self.raw_product) if self.raw_product else None,
            'raw_html' : str(self.raw_item) if self.raw_product else None,
            'categories' : ', '.join(self.categories), #string
            'source' : str(self.retailer),
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

=======
>>>>>>> dev

    __attrs__ = ['product_uuid', "product_id", "gtin", "item_uuid",
        "source", "name", "description", "images", "normalized",
        "categories", "url", "brand", "provider", "attributes",
        "ingredients","raw_html", "raw_product"]
    
    __extras__ = ['prod_attrs', 'prod_images', 'prod_categs']
    
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
            if self.attributes:
                self.save_attributes()
            # Save category, brand and provider as attributes            
            self.save_extras()
        except Exception as e:
            logger.error(e)
            raise errors.ApiError(70002, "Issues saving in DB!")
        return True

    def save_extras(self):
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
        self.save_attributes()

    def save_attributes(self):
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
            _exist = g._db.query("""SELECT EXISTS (
                        SELECT 1 FROM product_attr
                        WHERE product_uuid = '{}'
                        AND id_attr = {})"""\
                        .format(self.product_uuid, id_attr))\
                    .fetch()[0]['exists']
            # If not create product_attr
            if _exist:
                logger.info("Product Attr already in DB!")
                continue
            # Load model
            try:
                m_prod_at = g._db.model('product_attr', 'id_product_attr')
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

    def save_images(self):
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
    def update_image(p_obj):
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
                logger.info("Cannot update, image not in DB!")
                raise errors.ApiError(70006, "Cannot update, image not in DB!")
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

<<<<<<< HEAD
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
=======
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
            _cols = ','.join([x for x in kwargs['cols'].split(',') \
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
        if 'prod_attrs' in _cols:
            
>>>>>>> dev
