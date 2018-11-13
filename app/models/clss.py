import datetime
from flask import g
from app import errors, applogger
from config import *
import requests
import ast
import json

# Logger
logger = applogger.get_logger()

class Clss(object):
    """ Model for fetching clsses
    """

    __attrs__ = ['id_clss', 'name', 'name_es', 'match',
        'key', 'description', 'source']

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
            if not self.name_es:
                logger.error("Needs to specify at least one name in Clss!")
                raise Exception("Needs to specify at least one name in Clss!")
            self.name = self.name_es
        if not self.key:
            self.key = key_format(self.name)
        if not self.match:
            self.match = [self.key]

    def save(self, commit=True):
        """ Class method to save Clss in DB
        """
        logger.info("Saving clss...")
        if self.id_clss:
            if not Clss.exists({'id_clss': self.id_clss}):
                raise errors.ApiError(70006, "Cannot update, Clss not in DB!")
        elif Clss.exists({'key': self.key, 'source': self.source}):
            self.message = 'Clss already exists!'
            self.id_clss = Clss.get_id(self.key,
                                        self.source)
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
            exists = g._db.query("""SELECT EXISTS (
                            SELECT 1 FROM clss WHERE {})"""\
                            .format(_where))\
                        .fetch()[0]['exists']
        except Exception as e:
            logger.error(e)
            return False
        return exists
    
    @staticmethod
    def get_id(_key, _source):
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
            _res = g._db\
                    .query("""SELECT id_clss
                        FROM clss
                        WHERE key = '{}'
                        AND source = '{}'
                        LIMIT 1"""\
                        .format(_key,
                            _source))\
                    .fetch()
            if _res:
                return _res[0]['id_clss']
        except Exception as e:
            logger.error(e)
        return None
    