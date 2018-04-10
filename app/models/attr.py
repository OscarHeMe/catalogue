import datetime
from flask import g
from app import errors, logger
from config import *
import requests
import ast
import json
from app.models.clss import Clss
from app.norm.normalize_text import key_format

class Attr(object):
    """ Model for fetching attributes
    """

    __attrs__ = ['id_attr', 'id_clss', 'name', 'key',
        'match', 'has_value', 'meta', 'source']

    def __init__(self):
        """ Attr Constructor

            Params:
            -----
            _args : dict
                `Attr` model arguments
        """
        # Arguments verification and addition
        for _k in self.__attrs__:
            if _k in _args:
                self.__dict__[_k] = _args[_k]
                continue
            self.__dict__[_k] = None
        # Formatting needed params
        self.key = key_format(self.name)
        if not self.match:
            self.match = [self.key]
        

    