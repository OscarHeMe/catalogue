# -*- coding: utf-8 -*-
import app
import config
import unittest
import json
from pprint import pprint

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
    "brand": "Pepsi Cola",
    "provider": "Pepsico",
    "attributes": [{
        "attr_name": "Despensa",
        "attr_key": "despensa",
        "clss_name": "Categoría",
        "clss_key": "category",
    }],
    "raw_html": "<body>product_html</body>"
    # "item_uuid": "" # Missing
}

# Update product image
img_prod_test = {
    "image" : "http://chedraui.com.mx/media/catalog/product/1/2/124900_00.jpg",
    "descriptor" : [
        [1,2,3,4], [1,2,3,4]
    ]
}

# Cols to fetch on Test
cols_test = [
    'description', 'normalized', 'gtin',
    'raw_product', 'raw_html', 'categories',
    'ingredients', 'brand', 'provider', 'url', 'images',
    'prod_attrs', 'prod_images'] #  'prod_categs']

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
        new_prod_test['product_uuid'] = _jr['product_uuid']
    
    #@unittest.skip('Already tested')
    def test_05_modify_product(self):
        """ Modify existing Product
        """ 
        global new_prod_test
        print("Modify existing Product")
        new_prod_test['name'] = new_prod_test['name'].upper()
        _r =  self.app.post('/product/modify',
                data=json.dumps(new_prod_test),
                headers={'content-type':'application/json'})
        print(_r.status_code)
        try:
            _jr = json.loads(_r.data.decode('utf-8'))
            print(_jr)
        except:
            pass
        self.assertEqual(_r.status_code, 200)
    
    #@unittest.skip('Already tested')
    def test_06_update_prod_img(self):
        """ Update Product Image
        """ 
        global new_prod_test
        print("Update Product Image")
        img_prod_test['product_uuid'] = new_prod_test['product_uuid']
        _r =  self.app.post('/product/image',
                data=json.dumps(img_prod_test),
                headers={'content-type':'application/json'})
        print(_r.status_code)
        try:
            _jr = json.loads(_r.data.decode('utf-8'))
            print(_jr)
        except:
            pass
        self.assertEqual(_r.status_code, 200)
    
    #@unittest.skip('Already tested')
    def test_07_get_prods_by_item(self):
        """ Get Products by Item UUID (p=1, ipp=50)
        """ 
        print("Get Products by Item UUID (p=1, ipp=50)")
        _p, _ipp = 1, 50
        global new_item_test
        _r =  self.app.get('/product/by/iuuid?keys={}&cols={}&p={}&ipp={}'\
                .format('', #new_item_test['item_uuid'],
                    ','.join(cols_test),
                    _p, _ipp
                    )
                )
        print(_r.status_code)
        try:
            _jr = json.loads(_r.data.decode('utf-8'))
            pprint(_jr)
        except:
            pass
        self.assertEqual(_r.status_code, 200)
        #self.assertGreater(len(_jr), 0)
        #self.assertTrue(set(cols_test).issubset(_jr[0].keys()))

    #@unittest.skip('Already tested')
    def test_90_delete_product(self):
        """ Delete existing Product and its references
        """ 
        print("Delete existing Product and its references")
        global new_prod_test
        _r =  self.app.get('/product/delete?uuid='\
                        + new_prod_test['product_uuid'])
        print(_r.status_code)
        try:
            _jr = json.loads(_r.data.decode('utf-8'))
            print(_jr)
        except:
            pass
        self.assertEqual(_r.status_code, 200)


if __name__ == '__main__':
    unittest.main()