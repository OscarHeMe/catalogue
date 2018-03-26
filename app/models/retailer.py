import datetime
from flask import g
from app import errors, logger
from config import *
import requests
import ast
import json

class Source(object):
    """
        Class that queries retailer table
    """


    @staticmethod
    def get_all(retailer="byprice"):
        """ Get list of categories from given retailer
        """
        rows = g._db.query("select * from retailer order by name asc").fetch()
        return rows or []

