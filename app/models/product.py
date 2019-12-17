from app.models.category import Category
from app.models.attr import Attr
from app.models.formatter import Formatter
from app.norm.normalize_text import key_format, tuplify
from app.utils import errors
from ByHelpers import applogger
from config import *
from flask import g
import pandas as pd
from sqlalchemy import create_engine, Text
from sqlalchemy.dialects.postgresql import UUID
import datetime
import requests
import ast
import json
import uuid
from app.utils.postgresql_queries import *

from pprint import pprint

geo_stores_url = 'http://'+SRV_GEOLOCATION+'/store/retailer?key=%s'
logger = applogger.get_logger()


class Product(object):
    """ Class perform insert, update and query methods
        on PSQL Catalogue.product
    """
    _formatter_spec = {
        'gtin': 'int',
        'product_id': 'str',
        'name': 'str',
        'last_modified': 'str',
        'description' : 'str',
        'product_id' : 'str',
        'categories' : 'str', ## change to json new schema
        'images': 'json',
        'source': 'str',
        'url': 'str',
        'attributes': 'json',
        'ingredients': 'json',
        'last_modified' : 'str',
        'brand':'str',
        'provider': 'str',
        'is_outdated': 'bool'
    }

    _fmtr = Formatter(_formatter_spec)

    __attrs__ = [
        'product_uuid', "product_id", "gtin", "item_uuid",
        "source", "name", "description", "images",
        "categories", "url", "brand", "provider", "attributes",
        "ingredients", "raw_html", "raw_product", "is_outdated"
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
        # Format
        _args = self._fmtr.process(_args)
        try:
            # Arguments verification and addition
            for _k in self.__attrs__:
                if _k in _args:
                    self.__dict__[_k] = _args[_k]
                    continue
                self.__dict__[_k] = None
            # Args Aggregation
            self.last_modified = str(datetime.datetime.utcnow())
            self.gtin = str(self.gtin).zfill(14)[-14:] if self.gtin else None
            self.product_id = str(self.product_id).zfill(20)[-255:] \
                if self.product_id else None
            if len(self.name) > 250:
                self.name = self.name[:250]   
        except Exception as e:
            logger.error(e)
            if APP_MODE == "CONSUMER":
                logger.warning("Wrong DataType to serialize for Product ({} {})!".format(self.source, self.product_uuid))
                raise Exception("Wrong DataType to serialize for Product ({} {})!".format(self.source, self.product_uuid))
            if APP_MODE == "SERVICE":
                raise errors.ApiError(70005,
                    "Wrong DataType to serialize for Product!")
    

    def save(self, pcommit=True, _is_update=False, verified=False):
        """ Class method to save Product record in DB
            with product_image, product_attr and product_category
        """
        logger.debug("Saving Product...")
        # Verify for update
        if self.product_uuid:
            # If already validated for updated, dont do it again
            if _is_update:
                pass
            elif not Product.exists({'product_uuid': self.product_uuid}, commit=pcommit):
                # If wants to update but wrong UUID, return Error
                if APP_MODE == "CONSUMER":
                    logger.error("Cannot update, UUID not in DB ({} {})!".format(self.source, self.product_uuid))
                    return False
                if APP_MODE == "SERVICE":
                    raise errors.ApiError(70006,
                                          "Cannot update, UUID not in DB ({} {})!".format(self.source, self.product_uuid))
            _is_update = True
        # Verify for insert, if previously verified continue to save
        elif verified:
            pass
        else:
            # If not verified, check if not already in DB
            logger.debug('Getting product_uuid')
            if Product.exists({'product_id': self.product_id,
                             'source': self.source}, commit=pcommit):
                self.message = 'Product already exists!'
                logger.debug(self.message)
                self.product_uuid = Product\
                    .get({'product_id': self.product_id,
                        'source': self.source})[0]['product_uuid']
                return True
        logger.debug('Loading model')
        # Load model
        m_prod = g._db.model('product', 'product_uuid')
        for _k in self.__attrs__:
            if _k != 'attributes' and self.__dict__[_k]:
                m_prod.__dict__[_k] = self.__dict__[_k]
        # Add date
        m_prod.last_modified = str(datetime.datetime.utcnow())
        # Always add what Item UUID is set
        m_prod.item_uuid = str(self.item_uuid) if self.item_uuid else None
        step = 'Start'
        try:
            cmt = pcommit
            res = m_prod.save(commit=cmt)
            step = 'Product'
            self.message = "Correctly {} Product!"\
                .format('updated' if self.product_uuid else 'stored')
            if not self.product_uuid:
                self.product_uuid = m_prod.last_id
            logger.debug(self.message)
            # Save product images
            if self.images:
                self.save_images(pcommit=pcommit)
            # Save product categories
            if self.categories:
                self.save_categories(_is_update, pcommit=pcommit)
            # Save product attrs
            self.save_extras(_is_update, pcommit=pcommit)
        except Exception as e:
            m_prod.conn.commit()
            logger.error(e)
            if APP_MODE == "CONSUMER":
                logger.error("Issues saving in DB ({} {}). Step: {}".format(self.source, self.product_uuid, step))
                return False
            if APP_MODE == "SERVICE":
                raise errors.ApiError(70002, "Issues saving in DB ({} {}). Step: {}".format(self.source, self.product_uuid, step))
        return True

    def save_extras(self, update=False, pcommit=True):
        """ Class method to save brand, provider and categs
            as attributes
        """
        if not self.attributes:
            self.attributes = []
        # Load all elements as Attributes
        if self.brand:
            self.attributes.append({
                'attr_name': self.brand,
                'attr_key' : key_format(self.brand),
                'clss_name': 'Marca',
                'clss_key' : 'brand',
                'clss_desc': 'Marca'
            })
        if self.provider:
            self.attributes.append({
                'attr_name': self.provider,
                'attr_key' : key_format(self.provider),
                'clss_name': 'Proveedor',
                'clss_key' : 'provider',
                'clss_desc': 'Proveedor, Laboratorio, Manufacturador, etc.'
            })
        if self.categories:
            for _c in self.categories.split(','):
                self.attributes.append({
                    'attr_name': _c,
                    'attr_key': key_format(_c),
                    'clss_name': 'Categoría',
                    'clss_key': 'category',
                    'clss_desc': 'Categoría'
                })
        self.save_attributes(update, pcommit=pcommit)
        

    def save_attributes(self, update=False, pcommit=True):
        """ Class method to save product attributes
        """
        _nprs = {'attr_name', 'clss_name', 'attr_key', 'clss_key'}
        for _attr in self.attributes:
            # Validate attrs
            if not _nprs.issubset(_attr.keys()):
                logger.warning("Cannot add product attribute, missing keys!")
                continue
            # Verify if attr exists
            id_attr = Attr.get_id(_attr['attr_name'], self.source, commit=pcommit)
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
                        "description": _attr["clss_desc"]
                            if "clss_desc" in _attr else None,
                        "source": self.source}
                })
                id_attr = attr.save(commit=pcommit)
            # Verify if product_attr exists
            _qry = """SELECT id_product_attr
                    FROM product_attr
                    WHERE product_uuid = '{}'
                    AND id_attr = {} LIMIT 1
                    FOR UPDATE SKIP LOCKED""".format(self.product_uuid, id_attr)
            
            id_prod_attr = execute_select(g._psql_db.connection, _qry).fetchone()
            # If not create product_attr
            if id_prod_attr:
                id_prod_attr = id_prod_attr[0]
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
                if 'value' in _attr:
                    m_prod_at.value = _attr['value']
                if 'precision' in _attr:
                    m_prod_at.precision = _attr['precision']
                m_prod_at.last_modified = str(datetime.datetime.utcnow())
                m_prod_at.save(commit=pcommit)
                logger.debug("Product Attr correctly saved! ({})"
                            .format(m_prod_at.last_id))
            except Exception as e:
                logger.error(e)
                logger.warning("Could not save Product attr!")
        return True

    def save_images(self, pcommit=True):
        """ Class method to save product images
        """
        logger.debug('Saving all images')
        logger.debug(self.images)
        for _img in self.images:
            try:
                # Verify if prod image exists
                #qry_txt = """SELECT id_product_image FROM product_image
                #                        WHERE product_uuid = '{}'
                #                        AND image = '{}'""".format(self.product_uuid, _img)
                qry_txt = """SELECT id_product_image FROM product_image
                                        WHERE product_uuid = %s
                                        AND image = %s FOR UPDATE SKIP LOCKED"""
                # if '%' in qry_txt:
                #     g_qry = g._psql_db.cursor.execute(qry_txt.replace('%','%%'))    
                # else:        
                _exist = execute_select(g._psql_db.connection, qry_txt, (self.product_uuid, _img)).fetchone()
                if _exist:
                    _exist = _exist[0]
                    Product.save_pimage(self.product_uuid, _img, _exist, pcommit=pcommit)
                    continue
                # Load model
                Product.save_pimage(self.product_uuid, _img, pcommit=pcommit)
            except Exception as e:
                logger.error(e)
                logger.error(_img)
                logger.warning("Could not save Product image!")
        return True

    @staticmethod
    def save_pimage(p_uuid, _img, id_pim=None, descs=[], pcommit=False):
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
        m_prod_im.save(commit=pcommit)
        logger.debug("Product Image correctly saved! ({})"
                    .format(m_prod_im.last_id))
        return True

    @staticmethod
    def undo_match(puuid):
        """ Method to undo matching, by setting Item UUID
            of a given product to NULL
        """
        try:
            g._psql_db.cursor.execute("""UPDATE product
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
            id_pimg = g._psql_db.cursor.execute("""SELECT id_product_image
                    FROM product_image
                    WHERE product_uuid = '{}'
                    AND image = '{}'
                    LIMIT 1 
                    FOR UPDATE SKIP LOCKED"""\
                    .format(p_obj['product_uuid'], p_obj['image']))\
                .fetchall()
            if not id_pimg:
                if not or_create:
                    logger.warning("Cannot update, image not in DB!")
                    if APP_MODE == "CONSUMER":
                        return False
                    if APP_MODE == "SERVICE":
                        raise errors\
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
                raise errors\
                    .ApiError(70004, "Could not apply transaction in DB")
                return {
                    'status': "ERROR",
                    "message": "Could not apply transaction in DB"
                    }
    
    def save_categories(self, update=False, pcommit=True):
        """ Class method to save product categories
        """
        _parent = None
        for _cat in self.categories.split(','):
            try:
                # Get ID if exists, otherwise create category
                id_cat = Category.get_id(_cat, self.source, commit=pcommit)
                if not id_cat:
                    categ = Category({
                        'source': self.source,
                        'id_parent': Category.get_id(_cat,
                                                     self.source,
                                                     'id_parent', commit=pcommit),
                        'name': _cat
                        })
                    id_cat = categ.save(commit=pcommit)
                    # Emergency skip
                    if not id_cat:
                        continue
                # Verify product category does not exist
                _qry = """SELECT id_product_category
                        FROM product_category
                        WHERE id_category = {}
                        AND product_uuid = '{}' LIMIT 1""".format(id_cat, self.product_uuid)

                id_prod_categ = execute_select(g._psql_db.connection, _qry).fetchone()

                if id_prod_categ:
                    id_prod_categ = id_prod_categ[0]
                    if not update:
                        logger.info("Category already assigned to Product!")
                        continue
                    id_prod_categ = id_prod_categ[0]['id_product_category']
                m_prod_cat = g._db.model('product_category',
                                         'id_product_category')
                if id_prod_categ:
                    m_prod_cat.id_product_category = id_prod_categ
                m_prod_cat.product_uuid = self.product_uuid
                m_prod_cat.id_category = id_cat
                m_prod_cat.last_modified = str(datetime.datetime.utcnow())
                m_prod_cat.save(commit=pcommit)
                logger.debug("Product Category correctly saved! ({})"
                            .format(m_prod_cat.last_id))
            except Exception as e:
                logger.error(e)
                logger.warning("Could not save Product category!")
        return True

    @staticmethod
    def exists(k_param, commit=True):
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
            _q = """SELECT EXISTS (SELECT 1 FROM product 
                    WHERE {} LIMIT 1)""".format(_where) # FOR UPDATE SKIP LOCKED)""".format(_where)
            logger.debug("Query: {}".format(_q))
            exists = execute_select(g._psql_db.connection, _q).fetchone()[0]
        except Exception as e:
            logger.error('Error in func=exist')
            logger.error(e)
            return False
        return exists

    @staticmethod
    def select(k_param, cols=[], commit=True):
        """ Static method to verify Product existance

            Params:
            -----
            k_param : dict
                Key-value element to query in Product table
            cols : list
                Columns to get from query

            Returns:
            -----
            result : list of resulting elements from query
        """
        logger.debug("Verifying Product existance...")
        _where = ' AND '.join(["{}='{}'".format(*z)
                               for z in list(k_param.items())])
        try:
            _q = """SELECT {} FROM product 
                    WHERE {} """.format(','.join(cols),_where) # FOR UPDATE SKIP LOCKED)""".format(_where)
            logger.debug("Query: {}".format(_q))
            result = g._psql_db.cursor.execute(_q)\
                .fetchall()
        except Exception as e:
            logger.error(e)
            return False
        return result


    @staticmethod
    def insert_batch_qry(data_batch, table, pkey, cols=[]) -> list:
        values = []
        response = []
        p_uuids = []
        qry = ''
        if len(cols) > 0:
            for data in data_batch:
                # pprint(data)
                vs = []
                for k in cols:
                    value = data.get(k, None)
                    if isinstance(value, str) or isinstance(value, list):
                        value = "'" + str(value).replace('%', '%%').replace("'", "''") + "'"
                    elif not value:
                        value = 'NULL'

                    vs.append(str(value))

                if len(vs) > 0:
                    values.append("(" + ",".join(vs) + ")")

                if len(values) > 0:
                    qry = """INSERT INTO {} ({}) VALUES {} RETURNING {};""".format(table,
                                                                                ','.join(cols), 
                                                                                ','.join(values),
                                                                                pkey)
                    # logger.debug(qry[:1000])
                g._psql_db.cursor.execute(qry)
                response = g._psql_db.cursor.fetchall()
            for res in response:
                if len(res) > 0:
                    p_uuids.append(res[0])
        g._psql_db.connection.commit() 
        return p_uuids

    
    @staticmethod
    def update_prod_query(data_batch, table, pkey, cols=[]) -> list:
        values = []
        p_uuids = []
        sets = []
        tmp = []
        dic_vals = []

        #execute_bulk_insert(conn, query, values, template, page_size=BULK_INSERT_CHUNKSIZE)
        #execute_values(cursor, query, values, template=template, page_size=page_size)

        update_query = """UPDATE product AS prod
                    SET {}
                    FROM (VALUES %s) AS new({}) 
                    WHERE CAST (new.{} AS UUID) = prod.{};"""

        cols_change = cols.copy()
        cols_change.remove(pkey)

        if len(cols_change) > 0:
            for el in cols_change:
                if 'uuid' in el:
                    sets.append(' {} = CAST (new.{} AS UUID)'.format(el, el))
                else:
                    sets.append(' {} = new.{}'.format(el, el))

            qry = update_query.format(','.join(sets), ','.join(cols), pkey, pkey)

            for i in range(len(data_batch)):
                n_data = {}
                pval = data_batch[i].get(pkey)
                vs = []
                ks = []
                tmp = []
                d = {}
                for k in cols:
                    value = data_batch[i].get(k, None)
                    d[k] = value
                    tmp.append(' %({})s'.format(el))
                    # if isinstance(value, str) or isinstance(value, list):
                    #     value = "'" + str(value).replace('%', '%%').replace("'", "''") + "'"
                    #     value = str(value).replace('%', '%%').replace("'", "''")
                    # elif not value and not isinstance(value, bool):
                    #     continue

                    vs.append(str(value))
                    # ks.append(str(k))
                    # n_data[k] = value
                dic_vals.append(d)
                if vs:
                    # print(tp)
                    values.append(tuple(vs))
                    p_uuids.append(pval)
        print(qry)
        print(tuple(values)[0:2])
        if len(values) > 0:
            #print(qry)
            #print(values[:2])
            try:
                execute_bulk_insert(g._psql_db.connection, qry, tuple(dic_vals), '('+ ','.join(tmp) + ')')
            except Exception as e:
                logger.error('Error while trying to update {}:\n   - {}'.format([-1], e))             
        return p_uuids


    @staticmethod
    def save_all():
        g._psql_db.connection.commit()


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
        if _p['source'] in cached_ps.keys():
            if _p['product_id'] in cached_ps[_p['source']]:
                return [{'product_uuid': cached_ps[_p['source']][_p['product_id']]}]
        return None
    
    @staticmethod
    def create_cache_ids():
        """ Static method to initialize cache ids

            Returns:
            -----
            cache_ids : dict
                Nested dict by source and product_id to product_uuid map
        """
        qry = """SELECT product_uuid, source, product_id 
                 FROM product WHERE source NOT IN ('ims','plm','nielsen','gs1');"""
        qry = """SELECT product_uuid, source, product_id 
                 FROM product WHERE source IN (SELECT key 
                 FROM source WHERE retailer = '1');"""
        
        _df = pd.read_sql(qry, g._psql_db.connection)
        g._psql_db.connection.commit()        
        cache_ids = {}
        for y, gdf in _df.groupby('source'):
            cache_ids[y] = gdf[['product_uuid','product_id']]\
                            .set_index('product_id')\
                            .to_dict()['product_uuid']
        # Clean GC
        del _df
        return cache_ids

    @staticmethod
    def get(_by, _cols=['product_uuid'], limit=None, commit=True):
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
        items = []
        q_cols = ','.join(_cols) if _cols else 'product_uuid'
        _where = ' AND '.join(["{} IN {}"
                              .format(z[0], tuplify(z[1]))
                              for z in _by.items()])
        #logger.debug("Fetching products...")
        _query = "SELECT {} FROM product WHERE {}"\
            .format(q_cols, _where)
        if limit:
            _query += ' LIMIT {}'.format(limit)
        #logger.debug(_query)
        # print(_query)
        try:
            _items = execute_select(g._psql_db.connection, _query + ';').fetchall()
            for it in _items:
                d = {}
                for i in range(len(_cols)):
                    d[_cols[i]] = it[i]
                items.append(d.copy())
            #logger.debug("Got {} products".format(len(_items)))
        except Exception as e:
            logger.error(e)
            if APP_MODE == "CONSUMER":
                logger.error("Issues fetching elements in DB!")
                return False
            if APP_MODE == "SERVICE":
                raise errors.ApiError(70003, "Issues fetching elements in DB")
        return items

    @staticmethod
    def get_one(commit=True):
        """ Static Method to verify correct connection with Items Postgres DB
        """
        try:
            q = g._psql_db.cursor.execute("SELECT * FROM product LIMIT 1").fetchall()
        except:
            logger.error("Postgres Catalogue Connection error")
            return False
        for i in q:
            logger.debug('Product UUID: ' + str(i['product_uuid']))
        return {'msg': 'Postgres Items One Working!'}
    

    @staticmethod
    def query_count(_by, **params):
        """ Static method to query count by source

            Params:
            -----
            _by : str
                Key from which query is performed
            params : dict
                With opt keys to returnun out
            
            Returns:
            -----
            out: dict 
                With counts
               
        """
        # Build query
        _qry = """SELECT products, unique_items, items FROM (SELECT COUNT(*)  AS products, COUNT(DISTINCT(item_uuid))  AS unique_items, COUNT(item_uuid) as items FROM product WHERE {} = '{}' ) AS stt """\
            .format(_by, params['keys'])
        logger.debug(_qry)
        # Query DB
        try:
            _resp = g._psql_db.cursor.execute(_qry).fetchall()[0]
            logger.debug(_resp)
            logger.debug("Found {} products".format(_resp.get('products')))
            logger.debug("Found {} items".format(_resp.get('items')))
            out = {
                'product_uuids' : _resp.get('products'),
                'item_uuids' : _resp.get('items'),
                'unique_items': _resp.get('unique_items')
            }
        except Exception as e:
            logger.error(e)
            logger.warning("Issues fetching elements in DB!")
            if APP_MODE == "CONSUMER":
                return False
            if APP_MODE == "SERVICE":
                raise errors.ApiError(70003, "Issues fetching elements in DB")
        logger.debug(out)
        return out



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
            _cols = ','.join([x for x in Product.__base_q ])
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
        if _p < 1 :
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
        _qry = """SELECT {} FROM product {} ORDER BY {} OFFSET {} LIMIT {} FOR UPDATE SKIP LOCKED"""\
            .format(_cols, _keys, _orderby, (_p - 1)*_ipp, _ipp)
        logger.debug(_qry)
        # Query DB
        try:
            _resp = g._psql_db.cursor.execute(_qry).fetchall()
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
    def bulk_query(items, retailers, cols):
        """ Static method to bulk query by defined column values

            Params:
            -----
            items : str
                csv list of item_uuids
            retailers : str
                csv list of retailers
            cols : str
                csv list of columns to get from table
            
            Returns:
            -----
            _resp : list
                List of product objects
        """

        # Build query
        _qry = """SELECT {} FROM product WHERE item_uuid IN ({}) AND source IN ({})"""\
            .format(",".join(cols), "'" + "', '".join(items) + "'", "'" + "', '".join(retailers) + "'")
        logger.debug(_qry)
        # Query DB
        try:
            _resp = g._psql_db.cursor.execute(_qry).fetchall()
            logger.debug("Found {} products".format(len(_resp)))
        except Exception as e:
            logger.error(e)
            logger.warning("Issues fetching elements in DB!")
            raise errors.ApiError(70003, "Issues fetching elements in DB")

        return _resp



    @staticmethod
    def query_match(_by, **kwargs):
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
            _cols = ','.join([x for x in Product.__base_q ])
        # Format querying keys
        if kwargs['keys']:
            _keys = 'WHERE ' + _by + ' IN ' + str(tuplify(kwargs['keys']))
        else:
            if _by != 'product_uuid':
                _keys = 'WHERE {} IS NULL'.format(_by)
            else:
                _keys = ''
        # Add restriction
        if kwargs['items']:
            if kwargs['items'] == 'matched':
                if len(_keys) > 0:
                    _keys = _keys + ' AND '
                _keys = _keys + 'item_uuid IS NOT NULL'
            elif kwargs['items'] == 'notmatched':
                if len(_keys) > 0:
                    _keys = _keys + ' AND '
                _keys = _keys + 'item_uuid IS NULL'                    
        # Format paginators
        _p = int(kwargs['p'])
        if _p < 1 :
            _p = 1
        _ipp = int(kwargs['ipp'])
        # if _ipp > 5000:
        #     _ipp = 5000
        # Order by statement
        if 'orderby' in kwargs:
            _orderby = kwargs['orderby'] if kwargs['orderby'] else 'product_uuid'
        else:
            _orderby = 'product_uuid'
        if _orderby not in Product.__base_q:
            _orderby = 'product_uuid'
        ext = "OFFSET {} LIMIT {}".format((_p - 1)*_ipp, _ipp)
        # Build query
        _qry = """SELECT {} FROM product {} ORDER BY {} {}"""\
            .format(_cols, _keys, _orderby, ext)
        logger.debug(_qry)
        # Query DB
        try:
            _resp = g._psql_db.cursor.execute(_qry).fetchall()
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
            ORDER BY product_uuid""".format(tuplify(p_uuids))
        logger.debug(_qry)
        try:
            resp_at = g._psql_db.cursor.execute(_qry).fetchall()
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
        for i in range(0,len(p_uuids), 1000):
            _qry = """SELECT product_uuid, normalized
                FROM product_normalized
                WHERE product_uuid IN {}""".format(tuplify(p_uuids[i: i+1000]))
            logger.debug(_qry)
            try:
                resp_norm = g._psql_db.cursor.execute(_qry).fetchall()
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
            ORDER BY product_uuid""".format(tuplify(p_uuids))
        logger.debug(_qry)
        try:
            resp_im = g._psql_db.cursor.execute(_qry).fetchall()
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
            ORDER BY product_uuid""".format(tuplify(p_uuids))
        logger.debug(_qry)
        try:
            resp_ca = g._psql_db.cursor.execute(_qry).fetchall()
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
            {keys} {vals} {rets}"""\
            .format(table=field.split('.')[0],
                    ref_table=valu.split('.')[0],
                    keys=_keys,
                    vals=_vals,
                    rets=_rets)
        logger.debug(f_query)
        try:
            _fres = g._psql_db.cursor.execute(f_query).fetchall()
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
            _exists = g._psql_db.cursor.execute("""SELECT EXISTS (
                                    SELECT 1 FROM {table}
                                    WHERE product_uuid = '{uuid}'
                                    AND id_{table} = {_id}"""
                                  .format(table=_table,
                                          uuid=_uuid,
                                          _id=_id))\
                                .fetchall()[0]['exists']
            if not _exists:
                return {
                    'message': "Product Image ID not in DB!"
                }
            # Delete from Product extra record
            g._psql_db.cursor.execute("""DELETE FROM {table}
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
            g._psql_db.cursor.execute("DELETE FROM product_image WHERE product_uuid='{}'"
                        .format(p_uuid))
            # Delete from Product Category
            g._psql_db.cursor.execute("DELETE FROM product_category WHERE product_uuid='{}'"
                        .format(p_uuid))
            # Delete from Product Attr
            g._psql_db.cursor.execute("DELETE FROM product_attr WHERE product_uuid='{}'"
                        .format(p_uuid))
            # Delete from Product
            g._psql_db.cursor.execute("DELETE FROM product WHERE product_uuid='{}'"
                        .format(p_uuid))
        except Exception as e:
            logger.error(e)
            if APP_MODE == 'SERVICE':
                raise errors\
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
        if not set({'product_uuid', 'normalized'})\
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
            df[['product_uuid', 'normalized']]\
                .set_index('product_uuid')\
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
        # print(kwargs)
        if 'p' in kwargs:
            p = int(kwargs['p'][0])
            del kwargs['p']
        else:
            p = 1
        if 'ipp' in kwargs:
            ipp = int(kwargs['ipp'][0])
            del kwargs['ipp']
        else:
            ipp = 100

        # Columns
        if 'cols' not in kwargs or not kwargs['cols']:
            cols = [
                "i.name as name",
                "i.gtin as gtin", 
                "i.item_uuid as item_uuid",
                "p.*"
            ]
        else:
            cols = kwargs['cols'][0].split(",")
            del kwargs['cols']

        for i,c in enumerate(cols):
            if c in ['name','gtin','item_uuid','description']:
                cols[i] = "i.{} as {}".format(c, c)   

        # Replace keys
        for k in kwargs:
            if k in ['name','gtin','item_uuid']:
                new_key = "i.{}".format(k)
                kwargs[new_key] = kwargs[k]
                del kwargs[k]

        where = []
        where_qry = """ """
        for k, vals in kwargs.items():
            where.append(
                " {} IN ({}) ".format(
                    k, 
                    ",".join([ "'{}'".format(v) for v in vals ])
                ) 
            )

        if where:
            where_qry = """ where {}""".format(""" and """.join(where))

        # Query
        qry = """
            select {} from product p
            inner join item i on i.item_uuid = p.item_uuid
            {}
            limit {}
            offset {}
        """.format(
            """, """.join(cols),
            where_qry,
            ipp,
            (p - 1) * ipp
        )

        try:
            rows = g._psql_db.cursor.execute(qry).fetchall()
        except Exception as e:
            logger.error(e)
            logger.error("Could not execute intersect query: {}".format(qry))
            raise errors.ApiError(70007, "Could not execute query: ")

        return rows


    @staticmethod
    def upsert_id(item_uuid=None,source=None,new_product_id=None):
        """ Insert or update product id to match
        """
        prod = g._db.model('product','product_uuid')
        rows = g._psql_db.cursor.execute("""
            select product_uuid, product_id from product 
            where item_uuid = %s
            and source = %s
        """,(item_uuid, source)).fetchall()
        
        if rows:
            logger.info("Editing {} from {} to {}".format(
                rows[0]['product_uuid'], 
                rows[0]['product_id'],
                new_product_id
            ))
            p_uuid = rows[0]['product_uuid']
            prod.product_uuid = p_uuid
        else:
            logger.info("New product for {} id {}".format(
                source, new_product_id
            ))
            # Get item info to populate name
            _item = g._psql_db.cursor.execute("""
                select name, gtin from item
                where item_uuid = %s
            """,(item_uuid,)).fetchall()
            # Values
            if _item:
                prod.name  = _item[0]['name']
                prod.gtin  = _item[0]['gtin']
            prod.item_uuid = item_uuid
            prod.source = source
        
        try:
            prod.product_id = new_product_id
            prod.save()
            prod.clear()
        except:
            prod.rollback()
            raise Exception("Could not save product")
        
        return True


    @staticmethod
    def update(product_uuid=None, product_id=None, item_uuid=None, key=None):
        """ Update either item_uuid or product_id
        """
        logger.debug("Updating 2")
        prod = g._db.model('product','product_uuid')
        if not product_uuid:
            logger.error("Missing params")
            logger.debug("Not saving")
            return False
        try:
            prod.product_uuid = product_uuid
            if key == 'product_id':
                prod.product_id = None if not product_id else product_id
            if key == 'item_uuid':
                prod.item_uuid = None if not item_uuid else item_uuid
            prod.save()
            logger.debug("Saved...")
            logger.info("Saved product")
        except Exception as e: 
            prod.rollback()
            logger.error(e)
            raise Exception("Could not save product")
        return True


    @staticmethod
    def get_list(p=1, ipp=100, q=None, sources=None, gtins=None, matched=None, order=False):
        """ Get list of products given certain parameters   
            like gtin, query_string, sources, if matched, etc...
        """
        
        where = []
        if gtins and len(gtins) > 0:
            where.append("""
                (p.gtin in ({}))
            """.format(
                """, """.join(
                    [ """ '{}' """.format(g.zfill(14)) for g in gtins ]
                )
            ))

        if sources and len(sources) > 0:
            where.append(""" 
                (p.source in ({}))
            """.format(""", """.join(
                    [ """ '{}' """.format(s) for s in sources ]
                )
            ))

        if q:
            try:
                iuuid = uuid.UUID(q)
                where.append("""
                    ( p.item_uuid = '{}' )
                """.format(
                    q
                ))
            except:  
                where.append("""
                    (lower(p.name) like '%%{}%%' 
                    or p.gtin like '%%{}%%' 
                    or p.product_id like '%%{}%%' )
                """.format(
                    q.replace(" ","%%").lower(),
                    q,
                    q
                ))

        if matched:
            where.append("""
                ( item_uuid is {} null )
            """.format(
                """ not """ if matched == '1' else """"""
            ))
                
        # Get list of items with query and all
        prod_rows = g._psql_db.cursor.execute("""
            select product_uuid, product_id, gtin, name, description, source, item_uuid
            from product p {} {}
            limit %s offset %s
        """.format(
            """ """ if not where else """where {}""".format(
                """ and """.join(where)
            ),
            """ """ if not order else """ order by p.name asc """
        ), (ipp ,(p-1)*ipp)).fetchall()

        # Get all sources
        row_srcs = g._psql_db.cursor.execute("""
            select key from source 
            order by key asc
        """).fetchall()
        srcs_base = list([ row['key'] for row in row_srcs ])
        srcs = [ r['key'] for r in row_srcs]

        if not prod_rows:
            return {"products":[],"sources":srcs,"sources_base" : srcs_base }

        resp = {
            'sources' : srcs,
            'products' : prod_rows,
            'sources_base' : srcs_base 
        }

        return resp
