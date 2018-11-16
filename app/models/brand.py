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
    def get_all(retailer="byprice", fields=['id_attr','name'],p=None,ipp=None):
        """ Get list of brands of a given retailer
        """

        if p and ipp:
            offset = """ OFFSET %s LIMIT %s  """ % ( (p-1)*ipp, ipp)
        else:
            offset = """ """

        rows = g._db.query("""
            SELECT """+ """, """.join(fields) +""" FROM attr

            WHERE id_clss IN (SELECT id_clss FROM clss WHERE key = 'brand' AND source= %s)
            ORDER BY name ASC
            """ + offset + """
        """, (retailer,)).fetch()        

        return rows or []


    @staticmethod
    def get_by_items(items):
        ''' Get brands by item_uuids
        '''
        # Get the brands
        brands = g._db.query("""
            SELECT * FROM attr

            WHERE id_clss IN (SELECT DISTINCT clss.id_clss FROM attr 
            LEFT JOIN product_attr ON product_attr.id_attr=attr.id_attr 
            LEFT JOIN product ON product.product_uuid=product_attr.product_uuid
            LEFT JOIN clss ON clss.id_clss=attr.id_clss
            WHERE product.item_uuid IN (""" +  (""", """.join(["%s" for i in items])) + """)
            AND clss.key = 'brand' AND clss.source= 'ims')

            ORDER BY name ASC
            """, items).fetch()
            
        if not brands:
            return []
        return brands