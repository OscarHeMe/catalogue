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

class Attr(object):
    """ Model for fetching attributes
    """

    __attrs__ = ['id_attr', 'id_clss', 'value', 'clss']

    def __init__(self, _args):
        """ Attr Constructor

            Params:
            -----
            _args : dict
                `Attr` model arguments
        """
        # Arguments verification and addition
        logger.debug("Init attr ...")
        for _k in self.__attrs__:
            if _k in _args:
                self.__dict__[_k] = _args[_k]
                continue
            self.__dict__[_k] = None
        # Formatting needed params

        # Verify Clss
        logger.debug("Verify Clss ...")
        if not self.id_clss:
            if not self.clss:
                logger.error("Clss not defined to generate Attr!")
                raise Exception("Missing Clss for Attr.")
            # Get ID: If no clss, create one
            self.id_clss = Clss.get_id(self.clss['key'], self.clss['name'])
            if not self.id_clss:
                self.clss = Clss(self.clss)
                self.id_clss = self.clss.save()
        logger.debug("Init attr finished!")
        
    def save(self, commit=True):
        """ Class method to save attr in DB
        """
        logger.info("Saving attr...")
        if self.id_attr:
            if not Attr.exists({'id_attr': self.id_attr}):
                raise errors.ApiError(70006, "Cannot update, Attr not in DB!")
        elif Attr.exists({'id_clss': self.id_clss, 'value': self.value}):
            self.message = 'Attr already exists!'
            self.id_attr = Attr.get_id(self.id_clss, self.value)
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
        _where = ' AND '.join(["{}='{}'".format(z[0], z[1].format("'", "''")) \
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
    def get_id(id_clss, value, is_key=False):
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
                qry_string = """SELECT id_attr
                        FROM attr a
                        INNER JOIN clss c on c.id_clss=a.id_clss                  
                        WHERE c.key = '{}'
                        AND a.value = '{}'
                        LIMIT 1""".format(id_clss, value.replace("'", "''"))
            else:
                qry_string = """SELECT id_attr
                        FROM attr                   
                        WHERE id_clss = {}
                        AND value = '{}'
                        LIMIT 1""".format(id_clss, value.replace("'", "''"))
            logger.debug("Getting attr id: {}".format(qry_string))
            _res = g._db.query(qry_string).fetch()
            if _res:
                return _res[0]['id_attr']
        except Exception as e:
            logger.error("Error getting attr id: {}".format(str(e)))
        return None