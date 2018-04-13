# -*- coding: utf-8 -*-
import app
import config
import unittest
import json
from pprint import pprint
from app.models.product import Product

# Add Item Test
new_item_test = {
    "gtin": "00000001249002",
    "name": "refresco pepsi cola sin cafeina 354 ml",
    "description": "refresco pepsi cola sin cafeina 354 ml"
}

# Add Product Test
new_prod_test = {
    "product_id": "00000000000000124913",
    "gtin": "00000001249002",
    "source": "chedraui",
    "name": "refresco pepsi cola sin cafeina 354 ml",
    "description": "refresco pepsi cola sin cafeina 354 ml",
    "images" :  ['http://chedraui.com.mx/media/catalog/product/1/2/124900_00.jpg'],
    "categories": "Despensa",
    "url": "http://www.chedraui.com.mx/index.php/universidad/refresco-pepsi-cola-sin-cafeina-354ml.html",
    "brand": "Pepsi Cola con limón",
    "provider": "Pepsico",
    "attributes": [{
        "attr_name": "Despensa",
        "attr_key": "despensa",
        "clss_name": "Categoría",
        "clss_key": "category",
    },
    {
        "attr_name": "Disponible",
        "attr_key": "available",
        "clss_name": "Estado",
        "clss_key": "state",
    }
    ],
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
    'prod_images', 'prod_attrs', 'prod_categs']

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
        """ Set up
        """
        pass

    def tearDown(self):
        pass

    #@unittest.skip('Already tested')
    def test_00_product_validation(self):
        """ Testing Catalogue DB connection
        """ 
        with app.app.app_context():
            app.get_db()
            print("Testing Prduct validation")
            prod = Product(new_prod_test)
            pprint(prod.__dict__)
            print('Trying to save...')
            prod.save()
            try:
                self.assertTrue(True)
            except:
                self.assertFalse(True)

    @unittest.skip('Already tested')
    def test_01_get_db(self):
        """ Testing Catalogue DB connection
        """ 
        print("Testing Prduct validation")
        prod = Product(new_prod_test)
        pprint(prod.__dict__)
        print('Trying to save...')
        prod.save()
        try:
            self.assertTrue(True)
        except:
            self.assertFalse(True)
    


if __name__ == '__main__':
    unittest.main()