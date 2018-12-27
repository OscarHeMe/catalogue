import datetime
from flask import g
from app import errors
from ByHelpers import applogger
from config import *
import requests
import ast
import json
from app.models.clss import Clss
from app.norm.normalize_text import key_format

# Logger
logger = applogger.get_logger()


class Nutriment(object):
    """ Model for fetching attributes
    """

    __attrs__ = ['id_nutriment', 'name', 'key']

    def __init__(self, _args):
        """ Attr Constructor

            Params:
            -----
            _args : dict
                `Attr` model arguments
        """
        # Arguments verification and addition
        logger.debug("Init nutr...")
        for _k in self.__attrs__:
            if _k in _args:
                self.__dict__[_k] = _args[_k]
                continue
            self.__dict__[_k] = None
        # Formatting needed params

        logger.debug("Init nutriment finished!")

    def save(self, commit=True):
        """ Class method to save attr in DB
        """
        logger.info("Saving attr...")
        if self.id_nutriment:
            if not Nutriment.exists({'id_nutriment': self.id_nutriment}):
                raise errors.ApiError(70006, "Cannot update, Nutriment not in DB!")
        elif Nutriment.exists({'key': self.key}):
            self.message = 'Nutriment already exists!'
            self.id_nutriment = Nutriment.get_id(self.key)
            return self.id_nutriment
        # Load model
        m_nutr = g._db.model("nutriment", "id_nutriment")
        for _at in self.__attrs__:
            if self.__dict__[_at]:
                m_nutr.__dict__[_at] = self.__dict__[_at]
        try:
            # Save record
            self.message = "Nutriment {} correctly!".format( \
                'updated' if self.id_nutriment else 'stored')
            m_nutr.save(commit=commit)
            self.id_nutriment = m_nutr.last_id
            logger.info(self.message \
                        + '({})'.format(self.id_nutriment))
            return self.id_nutriment
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
        _where = ' AND '.join(["{}='{}'".format(z[0], str(z[1]).replace("'", "''")) \
                               for z in list(k_param.items())])
        try:
            exists = g._db.query("""SELECT EXISTS (
                            SELECT 1 FROM nutriment WHERE {} LIMIT 1)""" \
                                 .format(_where)) \
                .fetch()[0]['exists']
        except Exception as e:
            logger.error(e)
            return False
        return exists

    @staticmethod
    def get_id(name, is_key=True):
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
            if is_key is True:
                _res = g._db \
                    .query("""SELECT id_nutriment
                        FROM nutriment n                
                        WHERE n.key = '{}'
                        LIMIT 1""" \
                           .format(name)).fetch()
            else:
                _res = g._db \
                    .query("""SELECT id_nutriment
                        FROM nutriment n                   
                        WHERE name = '{}'
                        LIMIT 1""" \
                           .format(name)).fetch()
            if _res:
                return _res[0]['id_nutriment']
        except Exception as e:
            logger.error(e)
        return None