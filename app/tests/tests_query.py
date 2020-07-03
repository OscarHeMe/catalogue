# -*- coding: utf-8 -*-
import time
import unittest
from pprint import pformat, pprint

from memory_profiler import profile

import app
import config
from app import logger
from app.models.models import Product


page = 4
ipp = 2000

def check_duration(func):
    def wrapper_duration(*args, **kwargs):
        t0 = time.clock()
        result = func(*args, **kwargs)
        tf = time.clock()
        diff = round(tf-t0, 3)
        logger.info(f'Func {func.__name__}, took {diff}')
        return result
    return wrapper_duration

class QueryCatalogueTestCase(unittest.TestCase):
    """ Test Case for Catalogue Service
    """

    @classmethod
    def setUpClass(cls):
        """ Initializes the database
        """
        # Define test database
        if config.TESTING:
            with app.app.app_context():
                app.initdb()

    @classmethod
    def tearDownClass(cls):
        """ Drops database
        """
        if config.TESTING:
            with app.app.app_context():
                app.dropdb()

    def setUp(self):
        """ Generating Flask App context for testing
        """
        print("\n***************************\n")
        self.app = app.app.test_client()

    def tearDown(self):
        pass

    # @unittest.skip('Already tested')
    @check_duration
    @profile
    def test_00_catalogue_orm_limit_offset(self):
        print('-------------------Query with orm limit offset --------------------')
        """ Testing Catalogue ORM limit offset
        """
        success, products = Product.get_all_products(page, ipp)
        logger.info(f'Got {len(products)} products')
        self.assertTrue(isinstance(products, list) and len(products) > 0)
        self.assertTrue(success)
        logger.debug(products[-1])
    

    # @unittest.skip('Already tested')
    @check_duration
    @profile
    def test_01_catalogue_orm_pagination(self):
        print('------------------- Query with orm pagination --------------------')
        """ Testing Catalogue ORM pagination
        """
        success, products = Product.paginate_all_products(page, ipp)
        logger.info(f'Got {len(products)} products')
        self.assertTrue(isinstance(products, list) and len(products) > 0)
        self.assertTrue(success)
        logger.debug(products[-1])

    # @unittest.skip('Already tested')
    @check_duration
    @profile
    def test_02_catalogue_cursor_scroll(self):
        print('------------------- Query with server cursor scroll --------------------')
        """ Testing Catalogue server cursor scroll
        """
        success, products = Product.sql_paginate_products(page, ipp)
        logger.info(f'Got {len(products)} products')
        self.assertTrue(isinstance(products, list) and len(products) > 0)
        self.assertTrue(success)
        logger.debug(products[-1])




if __name__ == '__main__':
    unittest.main()
