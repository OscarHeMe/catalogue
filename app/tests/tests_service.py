# -*- coding: utf-8 -*-
import app
import config
import unittest
import json

class CatalogueServiceTestCase(unittest.TestCase):
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
        print("***************************")
        self.app = app.app.test_client()

    def tearDown(self):
        pass


    #@unittest.skip('Already tested')
    def test_0_catalogue_connection(self):
        """ Testing Catalogue DB connection
        """ 
        print("Testing Catalogue DB connection")
        _r =  self.app.get('/item/test')
        print(_r.status_code)
        try:
            print(json.loads(_r.data.decode('utf-8')))
            self.assertTrue(True)
        except:
            self.assertFalse(True)

if __name__ == '__main__':
    unittest.main()