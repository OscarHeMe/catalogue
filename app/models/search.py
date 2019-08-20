import datetime
from flask import g
from app import errors, logger
from config import *
import requests
import ast
import json
import unicodedata
import re


class Search(object):
    """
        Model for fetching products to search
    """

    __attrs__ = ['name', 'source']
    __base_q = ['name']
    __extras__ = []

    def __init__(self, params):
        """ Prod to save constructor
            Params:
            -----
            params : dict
                Table fields to initialize model (name, etc.)
        """
        self.name = norm_name(params['name']) \
            if 'name' in params else None
        self.source = params['source'] \
            if 'source' in params else None


    @staticmethod
    def get_by_source(sources):
        ''' Get products by source
        '''
        sources = "'" + "','".join(sources.split(',')) + "'"
        # Get the products
        produts = g._db.query("""
            SELECT name FROM search_by_source

            WHERE source IN ({})
            """.format(sources)).fetch()
            
        if not produts:
            return []
        return produts


    def add(self):
        """ Class method to save Item record in DB
        """
        logger.info("Saving Item...")
        # Model
        model_nm = 'search_by_source'
        qry_fields = ['name', 'source']
        
        qry = "INSERT INTO {} (name, source) VALUES ( '{}', '{}');".format(model_nm, self.name, self.source)
        try:
            m_prod_search = g._db
            m_prod_search.query(qry)
            self.message = "Correctly stored product!"
            logger.info(self.message)
            return True
        except Exception as e:
            logger.error(e)
            raise errors.ApiError(70002, "Issues saving in DB!")
        return False
    

def norm_name(key):
    if isinstance(key, str):
        try:
            key = str(key).lower()
            key = unicodedata.normalize('NFKD', key).encode('ascii', 'ignore').decode('utf-8', 'ignore')
            key = re.sub(r'[^\w\s]', '', key)
        except Exception as e:
            logger.error('Coud not normalize name: {}'.format(e))
    return key