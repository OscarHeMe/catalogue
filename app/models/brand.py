import datetime
from flask import g
from app import errors, logger
from config import *
import requests
import ast
import json

class Brand(object):
    """
        Model for fetching brands
    """

    @staticmethod
    def get_all(retailer="byprice", fields=['brand_uuid','name'],p=None,ipp=None):
        """ Get list of brands of a given retailer
        """

        if p and ipp:
            offset = """ OFFSET %s LIMIT %s  """ % ( (p-1)*ipp, ipp)
        else:
            offset = """ """

        rows = g._db.query("""
            select """+ """, """.join(fields) +""" from brand 
            where retailer = %s 
            order by name asc
            """ + offset + """
        """, (retailer,)).fetch()        

        return rows or []


    @staticmethod
    def get_by_items(items, retailer='byprice'):
        ''' Get brands by item_uuids
        '''
        # Get the brands
        brands = g._db.query("""
            select * from brand 
            where retailer = 'ims'
            and brand_uuid in (
                select brand_uuid from item_brand
                where item_uuid in (""" +  (""", """.join(["%s" for i in items])) + """)
            ) """
            , items).fetch()
        if not brands:
            return []
        return brands