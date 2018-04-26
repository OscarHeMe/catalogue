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

