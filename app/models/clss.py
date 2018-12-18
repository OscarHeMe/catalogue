import datetime
from flask import g
from app import errors
from ByHelpers import applogger
from config import *
import requests
import ast
import json

# Logger
logger = applogger.get_logger()

class Clss(object):
    """ Model for fetching clsses
    """

    __attrs__ = ['id_clss', 'name', 'key',
        'has_value', 'has_qty', 'has_order', 'has_unit']

    def __init__(self, _args):
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
        if not self.name:
            logger.error("Needs to specify at least one name in Clss!")
            raise Exception("Needs to specify at least one name in Clss!")
        if not self.key:
            logger.error("Clss key wasn't denifed")
            raise Exception("Needs to specify at least one name in Clss!")

    def save(self, commit=True):
        """ Class method to save Clss in DB
        """
        logger.info("Saving clss...")
        if self.id_clss:
            if not Clss.exists({'id_clss': self.id_clss}):
                raise errors.ApiError(70006, "Cannot update, Clss not in DB!")
        elif Clss.exists({'key': self.key, 'name': self.name}):
            self.message = 'Clss already exists!'
            self.id_clss = Clss.get_id(self.key,
                                        self.name)
            return self.id_clss
        # Load model
        m_cls = g._db.model("clss", "id_clss")
        for _at in self.__attrs__:
            if self.__dict__[_at]:
                m_cls.__dict__[_at] = self.__dict__[_at]
        try:
            # Save record
            self.message = "Clss {} correctly!".format(\
                'updated' if self.id_clss else 'stored')
            m_cls.save(commit=commit)
            self.id_clss = m_cls.last_id
            logger.info(self.message \
                    + '({})'.format(self.id_clss))
            return self.id_clss
        except Exception as e:
            logger.error(e)
            return None

    @staticmethod
    def exists(k_param):
        """ Static method to verify Clss existance

            Params:
            -----
            k_param : dict
                Key-value element to query in Clss table

            Returns:
            -----
            exists : bool
                Existance flag
        """
        logger.debug("Verifying Clss existance...")
        _where = ' AND '.join(["{}='{}'".format(*z) \
                            for z in list(k_param.items())])
        try:
            query_str = """SELECT EXISTS (SELECT 1 FROM clss WHERE {})""".format(_where)
            logger.debug("Query: " + query_str)
            exists = g._db.query(query_str)
            logger.debug(exists)
            exists = exists.fetch()
            logger.debug("Exists" + str(exists))
            exists = exists[0]['exists']
        except Exception as e:
            logger.error(_where)
            logger.error("Error in exists Clss: {}".format(str(e)))
            return False
        return exists
    
    @staticmethod
    def get_id(_key, name):
        """ Fetch ID from Clss by key

            Params:
            -----
            _key : str
                Clss key
            _source : str
                Source key

            Returns:
            -----
            _id : int
                Clss ID
        """        
        try:
            logger.debug("Getting id clss...")
            _res = g._db\
                    .query("""SELECT id_clss
                        FROM clss
                        WHERE key = '{}'
                        AND name = '{}'
                        LIMIT 1"""\
                        .format(_key,
                                name))\
                    .fetch()
            if _res:
                return _res[0]['id_clss']
        except Exception as e:
            logger.error("Error getting id clss: {}".format(str(e)))
        return None
    