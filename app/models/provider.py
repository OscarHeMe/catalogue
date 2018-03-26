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
    def get_all(retailer="byprice",fields=['provider_uuid','name'],p=None,ipp=None):
        """ Get list of categories from given retailer
        """
        if p and ipp:
            offset = """ OFFSET %s LIMIT %s  """ % ( (p-1)*ipp, ipp)
        else:
            offset = """ """

        rows = g._db.query("""
            select """+ """, """.join(fields) +""" from provider 
            where retailer = %s 
            order by name asc
            """ + offset + """
        """, (retailer,)).fetch()       
        return rows or []

