import datetime
from flask import g
from app import errors, logger
from config import *
import requests
import ast
import json

class Source(object):
    """ Class that queries source table
    """

    __attrs__ = ['key', 'name', 'logo', 'type', 'retailer', 'hierarchy']

    def __init__(self, _args):
        """ Source Constructor

            Params:
            -----
            _args : dict
                `Source` model arguments
        """
        # Arguments verification and addition
        for _k in self.__attrs__:
            if _k in _args:
                self.__dict__[_k] = _args[_k]
                continue
            self.__dict__[_k] = None
        # Formatting needed params
        if not self.key:
            raise Exception("Missing Source key")
        
    
    def save(self, commit=True):
        """ Class method to save attr in DB
        """
        logger.info("Saving source...")
        # Load model
        m_src = g._db.model("source", "key")
        for _at in self.__attrs__:
            if self.__dict__[_at]:
                m_src.__dict__[_at] = self.__dict__[_at]
        try:
            # Save record
            self.message = "Source {} correctly!".format(\
                'updated' if self.key else 'stored')
            m_src.save(commit=commit)
            self.key = m_src.last_id
            logger.info(self.message \
                    + '({})'.format(self.key))
            return self.key
        except Exception as e:
            logger.error(e)
            return None

    @staticmethod
    def get_all(_cols=''):
        """ Get all sources
        """
        _min_cols = ['name', 'key']
        if _cols:
            _fields = ','.join([x for x in \
                            (_cols.split(',') \
                            + _min_cols) \
                        if x in Source.__attrs__])
        else:
            _fields = ','.join([x for x in _min_cols ])
        try:
            rows = g._db.query("SELECT {} FROM source"\
                                .format(_fields))\
                        .fetch()
        except Exception as e:
            logger.error(e)
            return []
        return rows


    @staticmethod
    def get_products(**kwargs):
        """ Get catalogue
        """
        p = int(kwargs['p'])
        ipp = int(kwargs['ipp'])
        del kwargs['p'], kwargs['ipp']

        # Columns
        if 'cols' not in kwargs or not kwargs['cols']:
            cols = ["*"]
        else:
            print(kwargs['cols'])
            cols = [
                "i.item_uuid as item_uuid", 
                "p.product_uuid as product_uuid", 
                "i.gtin as gtin"
            ] 
            cols += [ c for c in kwargs['cols'].split(",") \
                    if c not in ['item_uuid', 'product_uuid', 'gtin'] ] 

            del kwargs['cols']

        where = []
        where_qry = """ """
        
        # Source query
        source = kwargs['source']
        where.append(""" p.source = '{}'  """.format(source))
        del kwargs['source']

        # Extras
        for k, vals in kwargs.items():
            where.append(""" {} IN ({}) """.format(k, vals) )

        if where:
            where_qry = """ where {}""".format(""" and """.join(where))

        # Query
        qry = """
            select {} from product p
            inner join item i
            on i.item_uuid = p.item_uuid
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
            rows = g._db.query(qry).fetch()
        except Exception as e:
            logger.error("Could not execute source catalogue query: {}".format(qry))
            raise errors.ApiError(70007, "Could not execute query: ")

        return rows


    @staticmethod
    def update_sources():
        rows = g._db.query("""
            select distinct source from product p
        """).fetch()
        sources = [r['source'] for r in rows]
        with open('data/sources.json','w') as file:
            json.dump(source, file)
        return True

        
    @staticmethod
    def get_sources():
        try:
            with open('data/sources.json') as file:
                json.load(file)
        except:
            update_sources()
            with open('data/sources.json') as file:
                json.load(file)
        return sources