import datetime
from flask import g
from app.utils import errors
from ByHelpers import applogger
from config import *
import pandas as pd
import requests
import ast
import json
from app.norm.normalize_text import key_format, tuplify
from uuid import UUID as ConstructUUID
from app.utils.gtin import *

geo_stores_url = 'http://' + SRV_GEOLOCATION + '/store/retailer?key=%s'
logger = applogger.get_logger()


class Item(object):
    """ Class perform Query methods on PostgreSQL items
    """

    __attrs__ = ['item_uuid', 'gtin', 'checksum', 'name',
                 'description', 'last_modified']
    __base_q = ['item_uuid', 'name', 'gtin']
    __extras__ = []

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
        try:
            m_item.checksum = int(self.gtin[-1])
        except:
            m_item.checksum = None
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
                            + Item.__base_q) \
                        if x in Item.__attrs__])
        else:
            _cols = ','.join([x for x in Item.__base_q ])
        # Format querying keys
        if kwargs['keys']:
            _keys = 'WHERE ' + _by + ' IN ' + str(tuplify(kwargs['keys']))
        else:
            if _by != 'item_uuid':
                _keys = 'WHERE {} IS NULL'.format(_by)
            else:
                _keys = ''
        # Format paginators
        _p = int(kwargs['p'])
        if _p < 1 :
            _p = 1
        _ipp = int(kwargs['ipp'])
        # if _ipp > 10000:
        #     _ipp = 10000
        # Order by statement
        if 'orderby' in kwargs:
            _orderby = kwargs['orderby'] if kwargs['orderby'] else 'item_uuid'
        else:
            _orderby = 'item_uuid'
        if _orderby not in Item.__base_q:
            _orderby = 'item_uuid'
        # Build query
        _qry = """SELECT {} FROM item {} ORDER BY {} OFFSET {} LIMIT {} """\
            .format(_cols, _keys, _orderby, (_p - 1)*_ipp, _ipp)
        logger.debug(_qry)
        # Query DB
        try:
            _resp = g._db.query(_qry).fetch()
            logger.info("Found {} items".format(len(_resp)))
        except Exception as e:
            logger.error(e)
            logger.warning("Issues fetching elements in DB!")
            if APP_MODE == "CONSUMER":
                return False
            if APP_MODE == "SERVICE":
                raise errors.ApiError(70003, "Issues fetching elements in DB")

        return _resp
    
    @staticmethod
    def it_list(**kwargs):
        """ Static method to get all items by query

            Params:
            -----
            kwargs : dict
                Extra arguments, such as (p, ipp, cols, etc..)
            
            Returns:
            -----
            _resp : list
                List of product objects
        """
        logger.debug('Querying items')
        logger.debug('fetching: {}'.format(kwargs))
        # Format columns
        if kwargs['cols']:
            _cols = ','.join([x for x in \
                            (Item.__base_q \
                            + kwargs['cols'].split(',')) \
                        if x in Item.__attrs__])
        else:
            _cols = ','.join([x for x in Item.__base_q ])
        # Format querying keys
        _keys = 'WHERE item_uuid IS NOT NULL'
        
        # Format paginators
        _p = int(kwargs['p'])
        if _p < 1 :
            _p = 1
        _ipp = int(kwargs['ipp'])
        if _ipp > 10000:
            _ipp = 10000
        # Order by statement
        if 'orderby' in kwargs:
            _orderby = kwargs['orderby'] if kwargs['orderby'] else 'item_uuid'
        else:
            _orderby = 'item_uuid'
        if _orderby not in Item.__base_q:
            _orderby = 'item_uuid'
        # Build query
        _qry = """SELECT {} FROM item {} ORDER BY {} OFFSET {} LIMIT {} """\
            .format(_cols, _keys, _orderby, (_p - 1)*_ipp, _ipp)
        logger.debug(_qry)
        # Query DB
        try:
            _resp = g._db.query(_qry).fetch()
            logger.info("Found {} items".format(len(_resp)))
        except Exception as e:
            logger.error(e)
            logger.warning("Issues fetching elements in DB!")
            if APP_MODE == "CONSUMER":
                return False
            if APP_MODE == "SERVICE":
                raise errors.ApiError(70003, "Issues fetching elements in DB")
        return _resp

    
    @staticmethod
    def it_count():
        """ Static method to count existent Items
        """
        _keys = 'WHERE item_uuid IS NOT NULL'
        # Build query
        _qry = """SELECT COUNT(*) as n_items FROM item {} """\
            .format(_keys)
        logger.debug(_qry)
        # Query DB
        try:
            _resp = g._db.query(_qry).fetch()
            logger.info("Found {} items".format(_resp[0]['n_items']))
        except Exception as e:
            logger.error(e)
            logger.warning("Issues fetching elements in DB!")
            if APP_MODE == "CONSUMER":
                return False
            if APP_MODE == "SERVICE":
                raise errors.ApiError(70003, "Issues fetching elements in DB")
        return _resp[0]['n_items']

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
        qry = """SELECT EXISTS (SELECT 1 FROM item WHERE {} = '{}');""".format(_key, _val)
        logger.debug(qry)
        try:
            exists = g._db.query(qry).fetch()[0]['exists']
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
        _query = "SELECT {} FROM item WHERE {} IN {}" \
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
    def get_by_gtin(gtins,  _cols=['item_uuid']):
        """ Static method to get Items by gtins
            Params:
            -----
            gtins : list of gtins
                Values to compare
            _cols : list
                Columns to retrieve
            Returns:
            -----
            _items : list
                List of elements
        """
        valid = []
        # Variations of gtin
        for gtin in gtins:
            try:
                code = str(int(gtin))
                valid.append(gtin)
            except Exception as e:
                logger.error("Not a valid gtin format")
                continue
         
        if not valid:
            raise Exception("No valid gtins")             
    
        try:
            iqry = """
                SELECT {}
                FROM item WHERE gtin IN ({})
            """.format(
                ",".join(_cols),
                ",".join( [ """'{}'""".format(v) for v in valid ] )
            )
            logger.debug(iqry)
            items = g._db.query(iqry).fetch()
            return items
        except Exception as e:
            logger.error(e)
            return []


    @staticmethod
    def get_by_like_gtin(gtins,  _cols=['item_uuid']):
        """ Static method to get Items by gtins
            Params:
            -----
            gtins : list of gtins
                Values to compare
            _cols : list
                Columns to retrieve
            Returns:
            -----
            _items : list
                List of elements
        """
        valid = []
        # Variations of gtin
        for gtin in gtins:
            try:
                code = str(int(gtin))
                valid.append(gtin)
            except Exception as e:
                logger.error("Not a valid gtin format")
                continue
         
        if not valid:
            raise Exception("No valid gtins")             
    
        try:
            items = []
            for v in valid:
                iqry = """
                    SELECT {}
                    FROM item WHERE gtin LIKE '{}'
                """.format(
                    ",".join(_cols), str(int(v))
                )
                logger.debug(iqry)
                items = items + g._db.query(iqry).fetch()
            return items
        except Exception as e:
            logger.error(e)
            return []


    @staticmethod
    def get_by_category(id_category,  _cols=['item_uuid'], p=1, ipp=200):
        """ Static method to get Items by gtins
            Params:
            -----
            gtins : list of gtins
                Values to compare
            _cols : list
                Columns to retrieve
            Returns:
            -----
            _items : list
                List of elements
        """
        # Get category details
        rc = g._db.query("""
            select * from category
            where id_category = %s
        """,(id_category,)).fetch()
        if not rc:
            raise Exception("No category found")

        try:
            cat = rc[0]
            qry = """
                select {}
                from item 
                where item_uuid in (
                    select item_uuid 
                    from product 
                    where product_uuid in (
                        select product_uuid 
                        from product_category 
                        where id_category  = {}
                    ) 
                )
                offset {} limit {}
            """.format(
                ",".join(_cols),
                id_category,
                (p - 1)*ipp, 
                ipp            
            )
            items = g._db.query(qry).fetch()      
        except Exception as e:
            logger.error(e)
            raise Exception(e)

        _resp = {
            "category" : cat,
            "items" : items
        }
        return _resp


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
            g._db.query("DELETE FROM item WHERE item_uuid='{}'" \
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
        return {'msg': 'Postgres Catalogue One Working!'}

    @staticmethod
    def get_elastic_items(params):
        """ Static Method to verify correct connection
            with Catalogue Postgres DB
        """
        items = params.get("items")
        type_ = params.get("type")
        if not items:
            logger.error("No items defined in params")
            return False
        if type_ == "item_uuid":
            try:
                qry_item_uuids = """
                    SELECT item_uuid, gtin, name as best_name, description, page_views
                        FROM item 
                        WHERE item_uuid IN {}
                    """.format(tuplify(items))
                df = pd.read_sql(qry_item_uuids, g._db.conn)
                qry_product_uuids = """
                    SELECT p.product_uuid, p.item_uuid, p.name name2, p.source, c.name class_name, 
                        c.key class_key,a.key attr_key,  a.name attr_name, pa.value
                            FROM product p
                            LEFT JOIN item_attr pa
                                ON p.item_uuid = pa.item_uuid
                            LEFT  JOIN attr a
                                ON a.id_attr = pa.id_attr
                            LEFT JOIN clss c
                                ON a.id_clss = c.id_clss
                        where p.item_uuid IN {}
                    """.format(tuplify(items))
                df2 = pd.read_sql(qry_product_uuids, g._db.conn)
                qry_categories="""
                    SELECT p.item_uuid, c.name as name_category, c.source
                        FROM product p
                        LEFT JOIN product_category pc on pc.product_uuid = p.product_uuid
                        INNER JOIN category c on c.id_category = pc.id_category
                        and p.item_uuid IN {}
                """.format(tuplify(items))
                df_categories=pd.read_sql(qry_categories, g._db.conn)
            except Exception as e:
                logger.error("Postgres Catalogue Connection error")
                logger.error(e)
                return False
            try:
                df['names'], df['retailers'], df['product_uuids'], df['attributes'], df['brands'], df['categories'], \
                df['ingredients'], df['providers'], df['categories_raw'] = None, None, None, None, None, None, None, None, None
                for index, row in df.iterrows():
                    row['names'] = list(df2[df2.item_uuid == row.item_uuid]["name2"].drop_duplicates())
                    row['retailers'] = list(df2[df2.item_uuid == row.item_uuid]["source"].drop_duplicates())
                    row['product_uuids'] = list(df2[df2.item_uuid == row.item_uuid]["product_uuid"].drop_duplicates())
                    row['attributes'] = list(df2[df2.item_uuid.isin([row.item_uuid]) & (~df2.attr_key.isnull()) & (~df2.attr_name.isnull())][
                                                 ['class_name', 'class_key', 'attr_key', 'attr_name',
                                                  'value']].T.to_dict().values())
                    row['brands'] = list(df2[df2.item_uuid.isin([row.item_uuid]) & (
                        ~df2.attr_key.isnull()) & (~df2.attr_name.isnull()) & df2.class_key.str.contains('brand')].drop_duplicates(
                        'attr_key').attr_name)
                    # All Categories Raw
                    row['categories_raw'] = list(df_categories[df_categories.item_uuid.isin([row.item_uuid]) &
                        (~df_categories.source.isin(["byprice", "byprice_farma"]))].name_category)
                    # All Categories
                    row['categories'] = list(set(df_categories[df_categories.item_uuid.isin([row.item_uuid]) &
                        (df_categories.source.isin(["byprice"]))].name_category))
                    row['ingredients'] = list(df2[df2.item_uuid.isin([row.item_uuid]) & (
                        ~df2.attr_key.isnull()) & (~df2.attr_name.isnull()) & df2.class_key.str.contains('ingredient')].drop_duplicates(
                        'attr_key').attr_name)
                    row['providers'] = list(df2[df2.item_uuid.isin([row.item_uuid]) & (
                        ~df2.attr_key.isnull()) & (~df2.attr_name.isnull()) & df2.class_key.str.contains('provider')].drop_duplicates(
                        'attr_key').attr_name)
                    row['tags'] = list(df2[df2.item_uuid.isin([row.item_uuid]) & (
                        ~df2.attr_key.isnull()) & (~df2.attr_name.isnull()) & df2.class_key.str.contains('tag')].drop_duplicates(
                        'attr_key').attr_name)
                    df.loc[index] = row
                items = list(df.T.to_dict().values())
            except Exception as e:
                logger.error("Error parsing the item")
                logger.error(e)
        elif type_ == "product_uuid":
            try:
                qry_product_uuids = """
                    SELECT p.product_uuid, p.gtin, p.description, p.item_uuid, p.name best_name, p.source, c.name class_name, 
                    c.key class_key,a.key attr_key,  a.name attr_name, pa.value
                        FROM product p
                        LEFT JOIN product_attr pa
                            ON p.product_uuid = pa.product_uuid
                        LEFT  JOIN attr a
                            ON a.id_attr = pa.id_attr
                        LEFT JOIN clss c
                            ON a.id_clss = c.id_clss
                    WHERE p.product_uuid IN {}
                    """.format(tuplify(items))
                df2 = pd.read_sql(qry_product_uuids, g._db.conn)
                qry_categories="""
                    SELECT p.product_uuid, c.name as name_category, c.source
                        FROM product p
                        LEFT JOIN product_category pc on pc.product_uuid = p.product_uuid
                        INNER JOIN category c on c.id_category = pc.id_category
                        and p.product_uuid IN {}
                """.format(tuplify(items))
                df_categories=pd.read_sql(qry_categories, g._db.conn)
            except Exception as e:
                logger.error("Postgres Catalogue Connection error")
                logger.error(e)
                return False
            try:
                df = df2.drop_duplicates('product_uuid')[['product_uuid', 'best_name', 'source', 'description', 'gtin']]
                df['names'], df['retailers'], df['product_uuids'], df['attributes'], df['brands'], df['categories'], \
                df['ingredients'], df['providers'], df['categories_raw'] = None, None, None, None, None, None, None, None, None
                for index, row in df.iterrows():
                    row['names'] = [row.best_name]
                    row['retailers'] = [row.source]
                    row['product_uuids'] = [row.product_uuid]
                    #row['attributes'] = list(df2[df2.product_uuid.isin([row.product_uuid]) & (~df2.attr_key.isnull()) & (~df2.attr_name.isnull())][
                    #                             ['class_name', 'class_key', 'attr_key', 'attr_name',
                    #                              'value']].T.to_dict().values())
                    row['attributes'] = []
                    row['brands'] = list(df2[df2.product_uuid.isin([row.product_uuid]) & (
                        ~df2.attr_key.isnull()) & (~df2.attr_name.isnull()) & df2.class_key.str.contains('brand')].drop_duplicates(
                        'attr_key').attr_name)
                    # Raw Categories
                    row['categories_raw'] = list(df_categories[df_categories.product_uuid.isin([row.product_uuid]) &
                        (~df_categories.source.isin(["byprice", "byprice_farma"]))].name_category)
                    # Categories ByPrice
                    row['categories'] = list(set(df_categories[df_categories.product_uuid.isin([row.product_uuid]) &
                        (df_categories.source.isin(["byprice"]))].name_category))
                    row['ingredients'] = list(df2[df2.product_uuid.isin([row.product_uuid]) & (
                        ~df2.attr_key.isnull()) & (~df2.attr_name.isnull()) & df2.class_key.str.contains('ingredient')].drop_duplicates(
                        'attr_key').attr_name)
                    row['providers'] = list(df2[df2.product_uuid.isin([row.product_uuid]) & (
                        ~df2.attr_key.isnull()) & (~df2.attr_name.isnull()) & df2.class_key.str.contains('provider')].drop_duplicates(
                        'attr_key').attr_name)
                    row['tags'] = list(df2[df2.product_uuid.isin([row.product_uuid]) & (
                        ~df2.attr_key.isnull()) & (~df2.attr_name.isnull()) & df2.class_key.str.contains('tag')].drop_duplicates(
                        'attr_key').attr_name)
                    df.loc[index] = row
                items = list(df.T.to_dict().values())
            except Exception as e:
                logger.error("Error parsing the item")
                logger.error(e)
                return False
        return items

    @staticmethod
    def get_catalogue_uuids(type_, offset_='0', limit_='ALL', last=False):
        """ Static Method to get the item_uuids and product_uuids from database
        """
        if type_ is None:
            try:
                catalogue = pd.read_sql("""
                    SELECT item_uuid as uuid, 'item_uuid' as type  
                        FROM item 
                    UNION ALL
                    SELECT product_uuid as uuid, 'product_uuid' as type 
                        FROM product 
                        WHERE item_uuid IS NULL
                    OFFSET {offset_}
                    LIMIT {limit_}
                    """.format(offset_=offset_, limit_=limit_), g._db.conn).to_dict(orient='records')
            except Exception as e:
                logger.error("Error while getting items: {}".format(e))
                return False
        elif type_=='product_uuid':
            try:
                catalogue = pd.read_sql("""
                    SELECT product_uuid
                        FROM product 
                        WHERE item_uuid IS NULL
                    OFFSET {offset_}
                    LIMIT {limit_}
                    """.format(offset_=offset_, limit_=limit_), g._db.conn).to_dict(orient='records')
            except:
                logger.error("Postgres Catalogue Connection error")
                return False
        elif type_=='item_uuid':
            try:
                catalogue = pd.read_sql("""
                    SELECT item_uuid  
                    FROM item
                    OFFSET {offset_}
                    LIMIT {limit_}
                    """.format(offset_=offset_, limit_=limit_), g._db.conn).to_dict(orient='records')
            except:
                logger.error("Postgres Catalogue Connection error")
                return False
        else:
            logger.error("Wrong type parameter {}".format(str(type_)))
            return False
        return ', '.join([str(dict_)for dict_ in catalogue])

    @staticmethod
    def get_total_items(type_):
        """ Static Method to get the item_uuids and product_uuids from database
        """
        if type_ is None:
            try:
                count_1 = pd.read_sql("""
                    SELECT count(item_uuid) as count_
                        FROM item 
                    """, g._db.conn)
                count_2 = pd.read_sql("""
                    SELECT count(product_uuid) as count_
                        FROM product 
                        WHERE item_uuid IS NULL
                    """, g._db.conn)
                count_ = int(list(count_1.count_)[0]) + int(list(count_2.count_)[0])

            except:
                logger.error("Postgres Catalogue Connection error")
                return False
        elif type_=='item_uuid':
            try:
                count_ = pd.read_sql("""
                    SELECT count(item_uuid) as count_
                        FROM item 
                    """, g._db.conn)
                count_ = int(list(count_.count_)[0])
            except:
                logger.error("Postgres Catalogue Connection error")
                return False
        elif type_=='product_uuid':
            try:
                count_ = pd.read_sql("""
                    SELECT count(product_uuid) as count_
                        FROM product 
                        WHERE item_uuid IS NULL
                    """, g._db.conn)
                count_ = int(list(count_.count_)[0])
            except:
                logger.error("Postgres Catalogue Connection error")
                return False
        else:
            logger.error("Wrong type parameter {}".format(str(type_)))
            return False
        return count_

    @staticmethod
    def fetch_attrs(puuids):
        """ Fetch all Attributes from given products

            Params:
            -----
            puuids : list
                List of Product UUIDs
            
            Returns:
            -----
            _respattrs : list
                List of attributes
            >>> [{
                    'class_name' : str,
                    'attr_name' : str,
                    'attr_key' : str,
                    'value': str
                }
            ]
        """
        # Query
        _qry = """SELECT
            pa.value, a.key as attr_key, 
            a.name as attr,
            c.name_es as class,
            c.key as class_key,
            a.source
            FROM product_attr pa
            LEFT OUTER JOIN attr a 
            ON (pa.id_attr = a.id_attr)
            LEFT OUTER JOIN clss c
            ON (a.id_clss = c.id_clss)
            WHERE pa.product_uuid IN {}
            ORDER BY class
        """.format(tuplify(puuids))
        try:
            logger.debug(_qry)
            _respattrs = pd.read_sql(_qry, g._db.conn).to_dict(orient='records')
            logger.debug(_respattrs)
        except Exception as e:
            logger.error(e)
            return []
        return _respattrs
    
    @staticmethod
    def fetch_categs(puuids):
        """ Fetch all ByPrice Categories from given products

            Params:
            -----
            puuids : list
                List of Product UUIDs
            
            Returns:
            -----
            bp_categs : list
                List of categories
            >>> ['Salud', 'Bebés']
        """
        # Query
        _qry = """SELECT name
            FROM category 
            WHERE id_category
            IN (
                SELECT id_category
                FROM product_category
                WHERE product_uuid IN {}
            )
            AND source = 'byprice'
        """.format(tuplify(puuids))
        try:
            logger.debug(_qry)
            bp_categs = pd.read_sql(_qry, g._db.conn).to_dict(orient='records')
            logger.debug(bp_categs)
        except Exception as e:
            logger.error(e)
            return []
        return [b['name'] for b in bp_categs]

    @staticmethod
    def details(u_type, _uuid):
        """ Method to query details from related Product or Item

            Params:
            -----
            u_type : str
                `item_uuid` or `product_uuid`
            _uuid :  str
                UUID to query info

            Returns:
            -----
            _details : dict
                Product/Item Details
        """
        item_uuid = None
        # Fetch info from all retailers
        try:
            if u_type == 'item_uuid':
                _qry = """SELECT i.name, i.gtin, i.description,
                    p.product_uuid, p.item_uuid as item_uuid,
                    p.images, p.ingredients, p.source,
                    s.hierarchy, s.retailer as show_label, s.name as r_name
                    FROM product p 
                    INNER JOIN source s 
                    ON (p.source = s.key)
                    INNER JOIN item i
                    ON (i.item_uuid = p.item_uuid)
                    WHERE p.{} = '{}';
                """.format(u_type, _uuid)
            else:
                _qry = """SELECT p.name, p.gtin, p.description,
                    p.product_uuid,
                    p.images, p.ingredients, p.source,
                    s.hierarchy, s.retailer as show_label,
                    s.name as r_name, p.item_uuid as item_uuid
                    FROM product p 
                    INNER JOIN source s 
                    ON (p.source = s.key)
                    WHERE p.{} = '{}';
                """.format(u_type, _uuid)
            logger.debug(_qry)
            info_rets = pd.read_sql(_qry, g._db.conn).to_dict(orient='records')
            logger.debug(info_rets)
            item_uuid = info_rets[0].get('item_uuid')
            if not info_rets:
                raise errors.ApiError(70008, "Not existing elements in DB!")
        except Exception as e:
            logger.error(e)
            logger.warning("Issues fetching retailers info: {}".format(_uuid))
            info_rets = []
        # Fetch Attributes
        if info_rets:
            info_attrs = Item.fetch_attrs([str(z['product_uuid']) for z in info_rets ])
        else:
            info_attrs = []
        # Aux vars
        prov, brand = {'name':'', 'key':''}, {'name':'', 'key':''}
        ingreds, attrs, categs = [], [], []
        ingred_options, categ_options = [], []
        # Fetch Provider, Brand, Categories
        for k in info_attrs:
            if k['class_key'] == 'provider':
                if len(k['attr']) > len(prov['name']):
                    prov = {
                        'name': k['attr'],
                        'key' : k['attr_key']
                    }
            elif k['class_key'] == 'brand':
                if k['source'] == 'byprice':
                    brand = {
                        'name': k['attr'],
                        'key' : k['attr_key']
                    }
            elif k['class_key'] == 'ingredient':
                if k['source'] == 'byprice':
                    ingreds.append(k['attr'])
                else:
                    ingred_options.append(k['attr'])
            elif k['class_key'] == 'category':
                if k['source'] == 'byprice':
                    categs.append(k['attr'])
                else:
                    categ_options.append(k['attr'])
            else:
                attrs.append(k)        
        # Fetch Attributes
        if info_rets:
            categs += Item.fetch_categs([str(z['product_uuid']) for z in info_rets ])
        # Add Normalized Ingredietns
        _normalized_attrs =  Item.fetch_normalized_attrs(u_type, _uuid)
        ingreds += _normalized_attrs['ingredients']
        if _normalized_attrs['brand']:
            brand = _normalized_attrs['brand']
        # Filter info from no valid retailers
        df_rets = pd.DataFrame(info_rets)
        if 'source' in df_rets.columns:
            df_rets = df_rets[~df_rets.source.isin(['ims','plm','gs1','nielsen'])]
            df_rets = df_rets[df_rets.show_label == 1]
        if df_rets.empty:
            raise errors.ApiError(70003, "Issues fetching elements in DB", 404)
        data = {
            'name': sorted(df_rets['name'].tolist(),
                key=lambda x: len(x) if x else 0,
                reverse=True)[0].strip().capitalize(),
            u_type: _uuid,
            'names': df_rets['name'].tolist(),
            'description': sorted(df_rets['description'].dropna().tolist(),
                key=lambda x: len(x) if x else 0, reverse=True),
            'gtin': sorted(df_rets['gtin'].dropna().tolist(),
                            key=lambda x: len(x) if x else 0)[0] \
                    if len(df_rets['gtin'].dropna()) > 0 \
                    else '',
            'retailers': df_rets['r_name'].tolist(),
            'attributes': attrs,
            'ingredients': ingreds,
            'ingredients_options': ingred_options,
            'brand': brand,
            'provider': prov,
            'categories': categs,
            'categories_options': categ_options
        }
        if item_uuid:
            data.update({'item_uuid':item_uuid})

        return data


    @staticmethod
    def fetch_normalized_attrs(u_type, _uuid):
        """ Static method to deliver ByPrice normalized Attributes

            Params:
            -----
            u_type : str
                Definition if Product or Item UUID
            _uuid : str
                UUID value
            
            Returns:
            -----
            n_attrs : dict
                Dicto containing list of Normalized Ingredients, and Normalized brand
        """
        if u_type  == 'item_uuid':
            _qry = """SELECT bpa.name, bpa.key, bpa.type
                FROM item_attr iat
                INNER JOIN 
                    (SELECT attr.id_attr, attr.name, attr.key, cl.key as type
                    FROM attr, clss cl
                    WHERE attr.id_clss = cl.id_clss
                    AND attr.source = 'byprice') AS bpa
                ON (bpa.id_attr = iat.id_attr)
                WHERE iat.item_uuid = '{}'
                AND (bpa.type = 'ingredient' OR bpa.type = 'brand')
            """.format(_uuid)
        else:
            _qry = """SELECT bpa.name, bpa.key, bpa.type
                FROM item_attr iat
                INNER JOIN 
                    (SELECT attr.id_attr, attr.name, attr.key, cl.key as type
                    FROM attr, clss cl
                    WHERE attr.id_clss = cl.id_clss
                    AND attr.source = 'byprice') AS bpa
                ON (bpa.id_attr = iat.id_attr)
                WHERE iat.item_uuid 
                IN (SELECT item_uuid FROM product WHERE product_uuid = '{}' LIMIT 1)
                AND (bpa.type = 'ingredient' OR bpa.type = 'brand')
            """.format(_uuid)
        logger.debug(_qry)
        # Vars
        n_attrs = {'ingredients': [], 'brand': {}}
        # Request elements
        try:
            df_nattrs = pd.read_sql(_qry, g._db.conn)
            if df_nattrs.empty:
                return n_attrs
            if not df_nattrs[df_nattrs['type'] == 'ingredient'].empty:
                n_attrs['ingredients'] = df_nattrs[df_nattrs['type'] == 'ingredient']['name'].tolist()
            if not df_nattrs[df_nattrs['type'] == 'brand'].empty:
                n_attrs['brand'] = df_nattrs[df_nattrs['type'] == 'brand']\
                                        [['name','key']].to_dict(orient='records')[0]
            logger.debug(n_attrs)
            return n_attrs
        except Exception as e:
            logger.error(e)
            return n_attrs

    @staticmethod
    def get_vademecum_info(_uuid):
        """ Static method to deliver info from Vademecum 
            by UUID

            Params:
            -----
            _uuid : str
                Item UUID
            
            Returns:
            -----
            _jresp = dict
                JSON like response
        """
        # Fetch info from DB
        try:
            # Validate UUID
            assert ConstructUUID(_uuid)
            # Query
            _qry = """SELECT * FROM item_vademecum_info
                WHERE item_uuid = '{}' 
                AND blacklisted = 'f' LIMIT 1""".format(_uuid)
            logger.debug(_qry)
            logger.info("Querying Vademecum info..")
            _resp = g._db.query(_qry).fetch()
            if len(_resp) == 0:
                raise Exception("No Item with vademecum info.")
            _data = json.loads(_resp[0]['data'])
        except Exception as e:
            logger.error(e)
            _data = {}
        _jresp = {
            "dataSources" : [ 
                    {
                    "source" : "vademecum",
                    "link" : "https://www.vademecum.es?utm_source=affiliate_source&utm_medium=web&utm_campaign=byprice",
                    "logo" : "https://s3-us-west-2.amazonaws.com/byprice-app/assets/backgrounds/vademecum-logo-mini.png",
                    "info" : "Fuente:",
                    "bullets" : _data
                    }
            ]
        }
        return _jresp


    @staticmethod
    def get_sitemap_items(size_, from_, farma=False, is_count=False):

        if farma:
            qry_join_categories = """
            INNER JOIN product_category pc ON p.product_uuid=pc.product_uuid
            """

            qry_categories = """
            AND pc.deprecated IS NULL
            AND (
                pc.id_category IN (                
                    SELECT id_category 
                    FROM category tmp 
                    WHERE source = 'byprice' 
                        AND (key='farmacia' OR key='jugos y bebidas')
                )
                OR p.source='farmacias_similares'
            )
            """
            qry_group = """GROUP BY p.product_uuid, p.name, p.description """
        else:
            qry_join_categories = ""
            qry_categories = ""
            qry_group = ""

        if is_count:
            qry_select_item ="""
                SELECT COUNT(DISTINCT(i.item_uuid)) AS count_, 'items' AS type_
            """
            qry_select_product = """
                SELECT COUNT(DISTINCT(p.product_uuid)) AS count_, 'products' AS type_
            """
            qry_group=""
        else:
            qry_select_item = """
                SELECT 
                    DISTINCT(i.item_uuid), 
                    NULL::UUID AS product_uuid,
                    i.name, 
                    i.description
            """
            qry_select_product ="""
                SELECT  
                    NULL as item_uuid, 
                    p.product_uuid, 
                    p.name, 
                    p.description
            """


        qry_item_uuids = """
            {qry_select_item}
                    FROM item i 
                    INNER JOIN product p ON i.item_uuid=p.item_uuid
                    {qry_join_categories}
                    WHERE p.source NOT IN ('gs1', 'ims', 'plm', 'mara')
                    AND p.item_uuid IS NOT NULL
                    {qry_categories}

            UNION ALL
            
            {qry_select_product}
                FROM product p
                    {qry_join_categories}
                    WHERE p.source NOT IN ('gs1', 'ims', 'plm', 'mara')
                    AND p.item_uuid IS NULL
                    {qry_categories}
                {qry_group}
            OFFSET {from_}
            LIMIT {size_}
            """.format(qry_select_item=qry_select_item, qry_select_product=qry_select_product, size_=size_, from_=from_,
                       qry_categories=qry_categories, qry_group=qry_group, qry_join_categories=qry_join_categories)
        logger.debug(qry_item_uuids)
        df = pd.read_sql(qry_item_uuids, g._db.conn)
        if is_count is False: # TODO Checar si debe hacerse esta validacion
            df['product_uuid'] = [[puuid] for puuid in df.product_uuid]
        return df

    
    @staticmethod
    def get_by_filters(filters):
        ''' Get catalogue by filters: item, category, retailer, providers
        '''
        c = []
        r = []
        i = []
        p = []
        b = []
        ing = []
        # Build query
        for obj in filters:
            [(f, v)] = obj.items()
            if f == 'category':
                c.append(v)
            elif f == 'retailer':
                r.append("""'""" + v + """'""")
            elif f == 'item':
                i.append("""'""" + v + """'""")
            elif f == 'provider':
                p.append("""'""" + v + """'""")
            elif f == 'brand':
                b.append("""'""" + v + """'""")
            elif f == 'ingredient':
                ing.append("""'""" + v + """'""")
        # Q by filters
        q = []
        if len(c) > 0:
            print(""", """.join(c))
            q.append(""" item_uuid in (select item_uuid from item_category where id_category in ( """ + """, """.join(
                c) + """ )) """)
        if len(r) > 0:
            q.append(""" item_uuid in (select item_uuid from item_retailer where retailer in ( """ + """, """.join(
                r) + """ )) """)
        if len(p) > 0:
            q.append(""" item_uuid in (select item_uuid from item_provider where provider_uuid in ( """ + """, """.join(
                p) + """ )) """)
        if len(b) > 0:
            q.append(""" item_uuid in (select item_uuid from item_brand where brand_uuid in ( """ + """, """.join(
                b) + """ )) """)
        if len(ing) > 0:
            q.append(
                """ item_uuid in (select item_uuid from item_ingredient where id_ingredient in ( """ + """, """.join(
                    ing) + """ )) """)
        if len(i) > 0:
            q.append(""" item_uuid in (""" + """, """.join(i) + """ ) """)
        where = (""" where """ + """ and """.join(q)) if len(q) > 0 else """ limit 100 """
        # Build query
        qry = """ select item_uuid, name, gtin from item """ + where
        items = g._db.query(qry).fetch()
        if not items:
            return []
        return items


    @staticmethod
    def get_list_ids(p=1, ipp=100, q=None, sources=None, gtins=None, display=None, order=False):
        """ Get list of products with respective 
            product_ids
        """
        
        where = []
        if gtins and len(gtins) > 0:
            where.append("""
                (i.gtin in ({}))
            """.format(
                """, """.join(
                    [ """ '{}' """.format(g.zfill(14)) for g in gtins ]
                )
            ))

        if sources and len(sources) > 0:
            where.append(""" 
                (i.item_uuid in (
                    select item_uuid from product
                    where source in ({}) 
                ))
            """.format(""", """.join(
                    [ """ '{}' """.format(s) for s in sources ]
                )
            ))

        if q:
            try:
                iuuid = ConstructUUID(q)
                where.append("""
                    ( i.item_uuid='{}' )
                """.format(
                    q
                ))
            except:  
                where.append("""
                    ( lower(i.name) like '%%{}%%' or i.gtin like '%%{}%%' )
                """.format(
                    q.replace(" ","%%"),
                    q
                ))
        
        # Get list of items with query and all
        items_rows = g._db.query("""
            select item_uuid, gtin, name 
            from item i
            {}
            {}
            limit %s 
            offset %s
        """.format(
            """ """ if not where else """where {}""".format(
                """ and """.join(where)
            ),
            """ """ if not order else """ order by name asc """
        ), (ipp ,(p-1)*ipp)).fetch()

        # Get all sources of the items
        row_srcs = g._db.query("""
            select key from source 
            order by key asc
        """).fetch()
        srcs_base = list([ row['key'] for row in row_srcs ])
        srcs = [ r['key'] for r in row_srcs if (not display or r['key'] in display) ]

        if not items_rows:
            return {"items":[],"sources":srcs,"sources_base":srcs_base}

        # Loop items
        items_results = []
        for item in items_rows:
            # get products of item
            prods_rows = g._db.query("""
                select product_id, source from product p
                where item_uuid = %s
            """, (item['item_uuid'],)).fetch()
            
            # Rows for sources
            sources_ids = { p['source'] : p['product_id'] for p in prods_rows }

            # Append result
            items_results.append({
                'item_uuid' : item['item_uuid'],
                'gtin' : item['gtin'],
                'name' : item['name'],
                'ids' : [ '' if s not in sources_ids else\
                         sources_ids[s] for s in srcs ]
            })

        resp = {
            'sources' : srcs,
            'items' : items_results,
            'sources_base' : srcs_base 
        }

        return resp

    @staticmethod
    def update(item_uuid=None, name=None, gtin=None):
        """ Update either name or gtin for given item_uuid
        """
        item = g._db.model('item','item_uuid')
        if not item_uuid:
            logger.error("Missing params")
            return False
        try:
            item.item_uuid = item_uuid
            if name != None:
                item.name = name
            if gtin != None:
                if is_valid_GTIN(int(gtin)):
                    item.gitn = str(gtin).zfill(14)
                else:
                    raise Exception("Invalid GTIN")
            item.save()
            logger.info("Saved item")
        except Exception as e: 
            item.rollback()
            logger.error(e)
            raise Exception("Could not save item")
        return True
