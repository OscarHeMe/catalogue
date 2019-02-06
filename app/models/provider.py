import datetime
from flask import g
from app import errors, logger
from config import *
import requests
import ast
import json

class Provider(object):
    """
        Class that queries retailer table
    """

    @staticmethod
    def get_all(retailer="byprice",fields=['id_attr','name'],p=None,ipp=None):
        """ Get list of categories from given retailer
        """
        if p and ipp:
            offset = """ OFFSET %s LIMIT %s  """ % ( (p-1)*ipp, ipp)
        else:
            offset = """ """

        rows = g._db.query("""
            SELECT """+ """, """.join(fields) +""" FROM attr

            WHERE id_clss IN (SELECT id_clss FROM clss WHERE key = 'provider' AND source= %s)
            ORDER BY name ASC
            """ + offset + """
        """, (retailer,)).fetch()       
        return rows or []

