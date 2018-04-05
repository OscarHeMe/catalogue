import json
import app.utils.db as db
import os
from pprint import pprint

# Retailer types
source_types = {
    'plm': 'data_provider',
    'mara': 'data_provider',
    'gs1': 'data_provider',
    'ims': 'data_provider',
    'byprice': 'master'
}

# Try to initialize DB
try:
    db.initdb()
except Exception as e:
    print(str(e))

# DB Connector and models
print('Connecting to Catalogue PSQL....')
_db = db.getdb()
print('Connected to Catalogue PSQL!')
msrc = _db.model("source","key")
matcls = _db.model("clss","id_clss")
mit = _db.model("item","item_uuid")
mcat = _db.model("category","id_category")
mat = _db.model("attr","id_attr")
#mpr = _db.model("product","product_uuid")
#mai = _db.model("attr_item")
#mii = _db.model("item_image","id_item_image")


def save_attr_classes(obj):
    """ Upsert `Clss` Table
    """
    _exists  = _db\
        .query("SELECT EXISTS (SELECT 1 FROM clss WHERE key = '{}')"\
            .format(obj['key'])).fetch()
    if _exists[0]['exists']:
        print('Clss already in DB!')
        return True
    # Load model
    matcls.id_clss = obj['id_attribute_class']
    matcls.name = obj['name']
    matcls.name_es = obj['name_es']
    matcls.match = obj['match']
    matcls.key = obj['key']
    if 'description' in obj:
        matcls.description = obj['description']
    if 'retailer' in obj:
        matcls.source = obj['retailer']
    try:
        matcls.save()
        print('Saved clss:', matcls.last_id)
        return True
    except Exception as e:
        print(e)
        return False

def save_attrs(obj):
    """ Upsert `Attr` Table
    """
    _exists  = _db\
        .query("SELECT EXISTS (SELECT 1 FROM attr WHERE key = '{}')"\
            .format(obj['key'])).fetch()
    if _exists[0]['exists']:
        print('Attr already in DB!')
        return True
    # Load model
    mat.id_attr = obj['id_attribute']
    mat.id_clss = obj['id_attribute_class']
    mat.name = obj['name']
    mat.key = obj['key']
    mat.match = obj['match']
    mat.has_value = 1 if obj['has_value'] else 0
    if 'meta' in obj:
        mat.source = obj['meta']
    if 'retailer' in obj:
        mat.source = obj['retailer']
    try:
        mat.save()
        print('Saved attr:', mat.last_id)
        return True
    except Exception as e:
        print(e)
        return False


def save_source(obj):
    """ Upsert `Source` Table
    """
    _exists  = _db\
        .query("SELECT EXISTS (SELECT 1 FROM source WHERE key = '{}')"\
            .format(obj['key'])).fetch()
    if _exists[0]['exists']:
        print('Source already in DB!')
        return True
    # Load model
    msrc.key = obj['key']
    msrc.name = obj['name']
    msrc.logo = obj['logo']
    msrc.type = source_types[obj['key']] \
        if obj['key'] in source_types else 'retailer'
    msrc.retailer = 0 if obj['key'] in source_types else 1
    msrc.hierarchy = obj['hierarchy']
    try:
        msrc.save()
        return True
    except Exception as e:
        print(e)
        return False

def save_gtin(obj):
    """ Upsert `Gtin` Table
    """
    _exists  = _db\
        .query("SELECT EXISTS (SELECT 1 FROM item WHERE item_uuid = '{}')"\
            .format(obj['item_uuid'])).fetch()
    if _exists[0]['exists']:
        print('Item already in DB!')
        return True
    # Loading model
    mit.item_uuid = str(obj['item_uuid'])
    mit.gtin = str(obj['gtin']).zfill(14)[-14:]
    mit.checksum = int(obj['checksum'])
    mit.name = obj['name']
    if 'description' in obj:
        mit.description = str(obj['description'])
    mit.last_modified = obj['date'] if 'date' in obj else obj['last_modified']
    try:
        mit.save()
        print('Saved item:', mit.last_id)
        return True
    except Exception as e:
        print(e)
        return False

def garanty_parent(id_cat):
    """ Verify if parent exists otherwise create provisional empty record
    """
    _exists  = _db\
        .query("SELECT EXISTS (SELECT 1 FROM category WHERE id_category = {})"\
            .format(id_cat)).fetch()
    if _exists[0]['exists']:
        print('Parent OK!')
        return True
    mcat.id_category = id_cat
    try:
        mcat.save()
        print('Created provisional parent:', mcat.last_id)
        return True
    except Exception as e:
        print(e)
        return False

def save_category(obj):
    """ Upsert `Category` Table
    """
    _exists  = _db\
        .query("SELECT EXISTS (SELECT 1 FROM category WHERE key = '{}')"\
            .format(obj['key'])).fetch()
    if _exists[0]['exists']:
        print('Category already in DB!')
        return True
    # Loading model
    if obj['id_parent'] and obj['id_parent'] != 0:
        garanty_parent(obj['id_parent'])
        mcat.id_parent = obj['id_parent']
    mcat.id_category = obj['id_category']
    mcat.source = obj['retailer']
    mcat.name = obj['name']
    mcat.key = obj['key']
    mcat.code = obj['code']
    try:
        mcat.save()
        print('Saved category:', mcat.last_id)
        return True
    except Exception as e:
        print(e)
        return False
    
def save_items(items):
    """ Loop products and save all information
    """
    print('Product format: ')
    pprint(items[:1])
    # Loop all items
    for gtin in items:
        # Save GTIN
        if not save_gtin(gtin):
            continue
        # Loop all products
        for prod in gtin['gtin_retailers']:
            pprint(prod)
            continue
            # Save the product information
            mpr.product_uuid = prod['item_uuid']
            mpr.gtin = prod['gtin']
            mpr.checksum = prod['checksum']
            mpr.name = prod['name']
            mpr.description = prod['description']
            mpr.date = prod['date']
            mpr.save()
            # Iterate over the items...
            if 'items' in prod and len(prod['items']) > 0:
                for item in prod['items']:

                    # Save the item
                    mit.source = item['retailer']
                    mit.product_uuid = item['item_uuid']
                    mit.url = item['url']
                    mit.name = item['name']
                    mit.description = item['description']
                    gtin = item['gtin']
                    mit.categories = item['categories']
                    mit.save()
                    print("Saved prod")
                    item_uuid = mit.last_id

                    # Save categories

                    # Save the images
                    
                    # Save all the attributes
                    if 'attributes' in item and item['attributes']:
                        for attr in item['attributes']:
                            # Check tne class / attribute
                            id_clss = check_clss(attr['clss_name'], attr['source'])
                            id_attr = check_attr(attr['attr_name'], attr['source'], id_clss)
                            # Save the attribute value
                            save_attr_item({
                                "id_attr" : id_attr,
                                "source" : attr['source'],
                                "value" : attr['value'],
                            })
        break ### Temp break

                    

if __name__ == '__main__':
    """
        Start loop to read the files of the 
        entire catalogue
    """
    print("Starting populator!")
    # Source upload
    with open('data/dumps/retailers.json', 'r') as _fr:
        for _k, _r in json.loads(_fr.read()).items():
            print('Loading:',_k)
            # Save source
            save_source(_r)
    print('Saved Sources!')
    # Category upload
    with open('data/dumps/categories.json', 'r') as _fr:
        _tl = list([_r for _k, _r in json.loads(_fr.read()).items()])
        for _r in sorted(_tl,
                        key=lambda d: d['id_parent'] if d['id_parent'] else 0,
                        reverse=False):
            print('Loading:', _r['key'])
            # Save category
            save_category(_r)
    print('Saved Categories!')
    # Attribute Class upload
    with open('data/dumps/attribute_classes.json', 'r') as _fr:
        for _k, _r in json.loads(_fr.read()).items():
            print('Loading:',_k)
            # Save attribute classes
            save_attr_classes(_r)
    print('Saved Clsses!')
    # Attributes upload
    with open('data/dumps/attributes.json', 'r') as _fr:
        for _k, _r in json.loads(_fr.read()).items():
            print('Loading:',_k)
            # Save attributes
            save_attrs(_r)
    print('Saved Attrs!')
    page = 1
    catalogue_page = []
    # While there is a file, open it
    for _file in os.listdir("data/dumps/"):
        if 'catalogue' not in _file:
            continue
        page = "data/dumps/" + str(_file)
        print("Opening page: {}".format(page))
        with open(page) as fobj:
            catalogue_page = json.load(fobj)
            print('Found', len(catalogue_page), 'products in page!')
            save_items(catalogue_page)
    print("Finished saving catalogue!")
    # Close connector
    _db.close()
    