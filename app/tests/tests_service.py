# -*- coding: utf-8 -*-
import app
import config
import unittest
import json

# Add Item Test
new_item_test = {
    "gtin": "00000001249002",
    "name": "refresco pepsi cola sin cafeina 354 ml",
    "description": "refresco pepsi cola sin cafeina 354 ml"
}

# Add Product Test
new_prod_test = {
    "product_id": "00000000000000124900",
    "gtin": "00000001249002",
    "source": "chedraui",
    "name": "refresco pepsi cola sin cafeina 354 ml",
    "description": "refresco pepsi cola sin cafeina 354 ml",
    "images" :  ['http://chedraui.com.mx/media/catalog/product/1/2/124900_00.jpg'],
    "categories": "Despensa",
    "url": "http://www.chedraui.com.mx/index.php/universidad/refresco-pepsi-cola-sin-cafeina-354ml.html",
    "brand": "", # Missing
    "provider": "", # Missing
    "attributes": [{
        "attr_name": "Despensa",
        "attr_key": "despensa",
        "clss_name": "Categoría",
        "clss_key": "category",
    }],
    "raw_html": "", # Missing
    # "item_uuid": "" # Missing
}

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
        print("\n***************************\n")
        self.app = app.app.test_client()

    def tearDown(self):
        pass

    @unittest.skip('Already tested')
    def test_00_catalogue_connection(self):
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
    
    #@unittest.skip('Already tested')
    def test_01_add_item(self):
        """ Add New Item
        """ 
        global new_item_test
        print("Add New Item")
        _r =  self.app.post('/item/add',
                data=json.dumps(new_item_test),
                headers={'content-type':'application/json'})
        print(_r.status_code)
        try:
            _jr = json.loads(_r.data.decode('utf-8'))
            print(_jr)
        except:
            pass
        self.assertEqual(_r.status_code, 200)
        new_item_test['item_uuid'] = _jr['item_uuid']
    
    #@unittest.skip('Already tested')
    def test_02_modify_item(self):
        """ Modify existing Item
        """ 
        print("Modify existing Item")
        global new_item_test
        _tmp_item = new_item_test
        _tmp_item['name'] = new_item_test['name'].upper()
        _r =  self.app.post('/item/modify',
                data=json.dumps(_tmp_item),
                headers={'content-type':'application/json'})
        print(_r.status_code)
        try:
            _jr = json.loads(_r.data.decode('utf-8'))
            print(_jr)
        except:
            pass
        self.assertEqual(_r.status_code, 200)
    
    #@unittest.skip('Already tested')
    def test_03_delete_item(self):
        """ Delete existing Item
        """ 
        print("Delete Item")
        global new_item_test
        _r =  self.app.get('/item/delete?uuid='\
                        + new_item_test['item_uuid'])
        print(_r.status_code)
        try:
            _jr = json.loads(_r.data.decode('utf-8'))
            print(_jr)
        except:
            pass
        self.assertEqual(_r.status_code, 200)

    #@unittest.skip('Already tested')
    def test_04_add_product(self):
        """ Add New Product
        """ 
        global new_prod_test
        print("Add New Product")
        _r =  self.app.post('/product/add',
                data=json.dumps(new_prod_test),
                headers={'content-type':'application/json'})
        print(_r.status_code)
        try:
            _jr = json.loads(_r.data.decode('utf-8'))
            print(_jr)
        except:
            pass
        self.assertEqual(_r.status_code, 200)

if __name__ == '__main__':
    unittest.main()