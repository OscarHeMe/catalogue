import datetime
from flask import g
from app import errors, logger
from config import *
import requests
import ast
import json

class Ingredient(object):
    """
        Model for ingredients and item ingredients
    """

    @staticmethod
    def get_all(retailer="byprice", fields=['id_ingredient','name']):
        """ Get list of ingredients of a given retailer
        """
        rows = g._db.query("select id_ingredient, name from ingredient where retailer = %s order by name asc", (retailer,)).fetch()        
        return rows or []

