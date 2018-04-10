import datetime
from flask import g
from app import errors, logger
from config import *
import requests
import ast
import json

class Clss(object):
    """ Model for fetching clsses
    """

    __attrs__ = ['id_clss', 'name', 'name_es', 'match',
        'key', 'description', 'source']

    def __init__(self):
        """ Clss constructor

            Params:
            -----
            _args : dict
                Clss model arguments
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
    