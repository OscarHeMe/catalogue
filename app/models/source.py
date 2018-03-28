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

    @staticmethod
    def get_all(source="byprice"):
        """ Get list of categories from given source
        """
        rows = g._db.query("select * from source order by name asc").fetch()
        return rows or []

