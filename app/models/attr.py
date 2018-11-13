import datetime
from flask import g
from app import errors, applogger
from config import *
import requests
import ast
import json
from app.models.clss import Clss
from app.norm.normalize_text import key_format

# Logger
logger = applogger.get_logger()

class Attr(object):
    """ Model for fetching attributes
    """

    __attrs__ = ['id_attr', 'id_clss', 'name', 'key',
        'match', 'has_value', 'meta', 'source', 'clss']

    def __init__(self, _args):
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
        if not self.key:
            self.key = key_format(self.name)
        if not self.match:
            self.match = [self.key]
        # Verify Clss
        if not self.id_clss:
            if not self.clss:
                logger.error("Clss not defined to generate Attr!")
                raise Exception("Missing Clss for Attr.")
            # Get ID: If no clss, create one
            self.id_clss = Clss.get_id(self.clss['key'], self.source)
            if not self.id_clss:
                self.clss = Clss(self.clss)
                self.id_clss = self.clss.save()
        
    def save(self, commit=True):
        """ Class method to save attr in DB
        """
        logger.info("Saving attr...")
        if self.id_attr:
            if not Attr.exists({'id_attr': self.id_attr}):
                raise errors.ApiError(70006, "Cannot update, Attr not in DB!")
        elif Attr.exists({'key': self.key, 'source': self.source}):
            self.message = 'Attr already exists!'
            self.id_attr = Attr.get_id(self.key,
                                        self.source)
            return self.id_attr
        # Load model
        m_atr = g._db.model("attr", "id_attr")
        for _at in self.__attrs__:
            if self.__dict__[_at]:
                m_atr.__dict__[_at] = self.__dict__[_at]
        try:
            # Save record
            self.message = "Attr {} correctly!".format(\
                'updated' if self.id_attr else 'stored')
            m_atr.save(commit=commit)
            self.id_attr = m_atr.last_id
            logger.info(self.message \
                    + '({})'.format(self.id_attr))
            return self.id_attr
        except Exception as e:
            logger.error(e)
            return None

    @staticmethod
    def exists(k_param):
        """ Static method to verify Attr existance

            Params:
            -----
            k_param : dict
                Key-value element to query in Attr table

            Returns:
            -----
            exists : bool
                Existance flag
        """
        logger.debug("Verifying Attr existance...")
        _where = ' AND '.join(["{}='{}'".format(*z) \
                            for z in list(k_param.items())])
        try:
            exists = g._db.query("""SELECT EXISTS (
                            SELECT 1 FROM attr WHERE {} LIMIT 1)"""\
                            .format(_where))\
                        .fetch()[0]['exists']
        except Exception as e:
            logger.error(e)
            return False
        return exists

    @staticmethod
    def get_id(_attr, _source):
        """ Fetch ID from attribute

            Params:
            -----
            _attr : str
                Attr name
            _source : str
                Source key

            Returns:
            -----
            _id : int
                Attr ID
        """        
        try:
            _res = g._db\
                    .query("""SELECT id_attr
                        FROM attr
                        WHERE key = '{}'
                        AND source = '{}'
                        LIMIT 1"""\
                        .format(key_format(_attr),
                            _source))\
                    .fetch()
            if _res:
                return _res[0]['id_attr']
        except Exception as e:
            logger.error(e)
        return None

    